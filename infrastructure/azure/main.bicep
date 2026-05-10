// ============================================================
// Smart Parking IoT Platform - Azure Bicep IaC
// Provisions: IoT Hub, Event Hub, Stream Analytics,
//             Cosmos DB, Azure Functions, API Management
// ============================================================

targetScope = 'resourceGroup'

@description('Environment name')
@allowed(['dev', 'staging', 'prod'])
param environment string

@description('Azure region for all resources')
param location string = resourceGroup().location

@description('Project name prefix')
@minLength(3)
@maxLength(15)
param projectName string = 'smartparking'

@description('IoT Hub SKU')
@allowed(['S1', 'S2', 'S3'])
param iotHubSku string = 'S1'

@description('IoT Hub units')
@minValue(1)
@maxValue(200)
param iotHubUnits int = 1

var namePrefix = '${projectName}-${environment}'
var tags = {
  Environment: environment
  Project: projectName
  ManagedBy: 'bicep'
  CostCenter: 'platform-engineering'
}

// ─── Log Analytics Workspace ───────────────────────────────
resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name: '${namePrefix}-logs'
  location: location
  tags: tags
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: 90
    workspaceCapping: { dailyQuotaGb: 10 }
  }
}

// ─── IoT Hub ───────────────────────────────────────────────
resource iotHub 'Microsoft.Devices/IotHubs@2023-06-30' = {
  name: '${namePrefix}-iothub'
  location: location
  tags: tags
  sku: {
    name: iotHubSku
    capacity: iotHubUnits
  }
  properties: {
    eventHubEndpoints: {
      events: {
        retentionTimeInDays: 7
        partitionCount: 32
      }
    }
    routing: {
      routes: [
        {
          name: 'occupancy-events'
          source: 'DeviceMessages'
          condition: 'type = "occupancy"'
          endpointNames: ['events']
          isEnabled: true
        }
        {
          name: 'alerts'
          source: 'DeviceMessages'
          condition: 'type = "alert"'
          endpointNames: ['events']
          isEnabled: true
        }
      ]
      fallbackRoute: {
        name: 'fallback'
        source: 'DeviceMessages'
        condition: 'true'
        endpointNames: ['events']
        isEnabled: true
      }
    }
    cloudToDevice: {
      maxDeliveryCount: 10
      defaultTtlAsIso8601: 'PT1H'
      feedback: {
        lockDurationAsIso8601: 'PT5S'
        ttlAsIso8601: 'PT1H'
        maxDeliveryCount: 10
      }
    }
    minTlsVersion: '1.2'
  }
}

// ─── IoT Hub Diagnostics ───────────────────────────────────
resource iotHubDiagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'iotHubDiagnostics'
  scope: iotHub
  properties: {
    workspaceId: logAnalytics.id
    logs: [
      { category: 'Connections'; enabled: true; retentionPolicy: { enabled: true; days: 30 } }
      { category: 'DeviceTelemetry'; enabled: true; retentionPolicy: { enabled: true; days: 30 } }
      { category: 'DeviceIdentityOperations'; enabled: true; retentionPolicy: { enabled: true; days: 90 } }
    ]
    metrics: [
      { category: 'AllMetrics'; enabled: true; retentionPolicy: { enabled: true; days: 30 } }
    ]
  }
}

// ─── Event Hub Namespace ───────────────────────────────────
resource eventHubNamespace 'Microsoft.EventHub/namespaces@2023-01-01-preview' = {
  name: '${namePrefix}-evhns'
  location: location
  tags: tags
  sku: {
    name: 'Standard'
    tier: 'Standard'
    capacity: 2
  }
  properties: {
    isAutoInflateEnabled: true
    maximumThroughputUnits: 20
    kafkaEnabled: true
    minimumTlsVersion: '1.2'
    zoneRedundant: true
  }
}

resource parkingTelemetryHub 'Microsoft.EventHub/namespaces/eventhubs@2023-01-01-preview' = {
  parent: eventHubNamespace
  name: 'parking-telemetry'
  properties: {
    messageRetentionInDays: 7
    partitionCount: 32
    status: 'Active'
  }
}

// ─── Cosmos DB (Parking State Store) ──────────────────────
resource cosmosAccount 'Microsoft.DocumentDB/databaseAccounts@2023-11-15' = {
  name: '${namePrefix}-cosmos'
  location: location
  tags: tags
  kind: 'GlobalDocumentDB'
  properties: {
    consistencyPolicy: {
      defaultConsistencyLevel: 'Session'
    }
    locations: [
      { locationName: location; failoverPriority: 0; isZoneRedundant: true }
    ]
    databaseAccountOfferType: 'Standard'
    enableAutomaticFailover: true
    backupPolicy: {
      type: 'Continuous'
      continuousModeProperties: { tier: 'Continuous7Days' }
    }
    publicNetworkAccess: 'Disabled'
    minimalTlsVersion: 'Tls12'
  }
}

resource cosmosDatabase 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2023-11-15' = {
  parent: cosmosAccount
  name: 'ParkingPlatform'
  properties: {
    resource: { id: 'ParkingPlatform' }
    options: { autoscaleSettings: { maxThroughput: 4000 } }
  }
}

resource parkingSpacesContainer 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2023-11-15' = {
  parent: cosmosDatabase
  name: 'ParkingSpaces'
  properties: {
    resource: {
      id: 'ParkingSpaces'
      partitionKey: { paths: ['/zoneId']; kind: 'Hash'; version: 2 }
      indexingPolicy: {
        indexingMode: 'consistent'
        includedPaths: [{ path: '/*' }]
        excludedPaths: [{ path: '/\"_etag\"/?'}]
        compositeIndexes: [
          [
            { path: '/zoneId'; order: 'ascending' }
            { path: '/status'; order: 'ascending' }
          ]
        ]
      }
      defaultTtl: -1
    }
  }
}

// ─── Storage Account ───────────────────────────────────────
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: replace('${namePrefix}storage', '-', '')
  location: location
  tags: tags
  sku: { name: 'Standard_ZRS' }
  kind: 'StorageV2'
  properties: {
    accessTier: 'Hot'
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    networkAcls: {
      bypass: 'AzureServices'
      defaultAction: 'Deny'
    }
  }
}

// ─── Azure Function App (Telemetry Processor) ─────────────
resource appServicePlan 'Microsoft.Web/serverfarms@2023-01-01' = {
  name: '${namePrefix}-asp'
  location: location
  tags: tags
  sku: { name: 'Y1'; tier: 'Dynamic' }
  properties: {}
}

resource functionApp 'Microsoft.Web/sites@2023-01-01' = {
  name: '${namePrefix}-func'
  location: location
  tags: tags
  kind: 'functionapp'
  identity: { type: 'SystemAssigned' }
  properties: {
    serverFarmId: appServicePlan.id
    httpsOnly: true
    siteConfig: {
      pythonVersion: '3.11'
      minTlsVersion: '1.2'
      appSettings: [
        { name: 'AzureWebJobsStorage'; value: 'DefaultEndpointsProtocol=https;AccountName=${storageAccount.name};AccountKey=${listKeys(storageAccount.id, storageAccount.apiVersion).keys[0].value}' }
        { name: 'FUNCTIONS_EXTENSION_VERSION'; value: '~4' }
        { name: 'FUNCTIONS_WORKER_RUNTIME'; value: 'python' }
        { name: 'COSMOS_ENDPOINT'; value: cosmosAccount.properties.documentEndpoint }
        { name: 'IOTHUB_CONNECTION_STRING'; value: listKeys(iotHub.id, iotHub.apiVersion).value[0].primaryConnectionString }
        { name: 'EVENTHUB_NAMESPACE'; value: '${eventHubNamespace.name}.servicebus.windows.net' }
        { name: 'APPINSIGHTS_INSTRUMENTATIONKEY'; value: appInsights.properties.InstrumentationKey }
      ]
      cors: {
        allowedOrigins: ['https://portal.azure.com']
        supportCredentials: false
      }
    }
  }
}

// ─── Application Insights ─────────────────────────────────
resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: '${namePrefix}-appinsights'
  location: location
  tags: tags
  kind: 'web'
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: logAnalytics.id
    RetentionInDays: 90
    IngestionMode: 'LogAnalytics'
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
}

// ─── Outputs ───────────────────────────────────────────────
output iotHubHostname string = '${iotHub.name}.azure-devices.net'
output eventHubNamespaceFQDN string = '${eventHubNamespace.name}.servicebus.windows.net'
output cosmosEndpoint string = cosmosAccount.properties.documentEndpoint
output storageAccountName string = storageAccount.name
output functionAppName string = functionApp.name
output logAnalyticsWorkspaceId string = logAnalytics.id
