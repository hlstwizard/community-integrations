# `dagster-contrib-azure`

## Test

```sh
make test
```

## Build

```sh
make build
```

## Overview

This package provides integrations with Microsoft Azure services. It currently includes the following
integrations:

### Container Instances

#### Azure Container Instances run launcher

Adds support for launching Dagster runs on Azure Container Instances. This launcher creates a new container instance for each Dagster run, providing isolated execution environments and automatic scaling.

## Prerequisites

Before using this launcher, ensure you have:

1. An Azure subscription with Container Instances enabled
2. A container registry (e.g., Azure Container Registry) with your Dagster job images
3. Appropriate Azure credentials configured (see Authentication section)
4. Resource groups created where container instances will be deployed

## Installation

```sh
pip install dagster-contrib-azure
```

Or add to your `requirements.txt`:
```
dagster-contrib-azure>=0.0.1
```

## Quick Start

1. **Build and push your Dagster image to a container registry:**
   ```dockerfile
   FROM python:3.11-slim
   
   # Install your Dagster code and dependencies
   COPY . /app
   WORKDIR /app
   RUN pip install -e .
   
   # Default command for Dagster execution
   CMD ["dagster", "api", "grpc", "--host", "0.0.0.0", "--port", "4000"]
   ```

2. **Configure your Dagster instance:**
   ```yaml
   # dagster.yaml
   run_launcher:
     module: dagster_contrib_azure.container_instances.run_launcher
     class: ContainerInstancesRunLauncher
     config:
       subscription_id:
         env: AZURE_SUBSCRIPTION_ID
       resource_group:
         env: AZURE_RESOURCE_GROUP
       location:
         env: AZURE_LOCATION
       container_image: "myregistry.azurecr.io/my-dagster-image:latest"
       container_group_by_code_location:
         my-code-location: my-container-group
   ```

3. **Set environment variables:**
   ```bash
   export AZURE_SUBSCRIPTION_ID="your-subscription-id"
   export AZURE_RESOURCE_GROUP="your-resource-group"
   export AZURE_LOCATION="eastus"
   ```

## Configuration

### Basic Configuration

```yaml
run_launcher:
  module: dagster_contrib_azure.container_instances.run_launcher
  class: ContainerInstancesRunLauncher
  config:
    subscription_id:
      env: AZURE_SUBSCRIPTION_ID
    resource_group:
      env: AZURE_RESOURCE_GROUP
    location:
      env: AZURE_LOCATION
    container_image: "myregistry.azurecr.io/dagster:latest"
    container_group_by_code_location:
      my-code-location: my-container-group
```

### Advanced Configuration

```yaml
run_launcher:
  module: dagster_contrib_azure.container_instances.run_launcher
  class: ContainerInstancesRunLauncher
  config:
    subscription_id:
      env: AZURE_SUBSCRIPTION_ID
    resource_group:
      env: AZURE_RESOURCE_GROUP
    location:
      env: AZURE_LOCATION
    container_image: "myregistry.azurecr.io/dagster:latest"
    cpu_limit: 2.0          # CPU cores per container
    memory_limit: 4.0       # GB of memory per container
    run_timeout: 7200       # Timeout in seconds (2 hours)
    
    container_group_by_code_location:
      # Simple configuration
      simple-location: my-container-group
      
      # Advanced configuration with overrides
      advanced-location:
        name: advanced-container-group
        subscription_id: different-subscription
        resource_group: different-rg
        location: westus
        # Environment variables for this location
        DATABASE_URL:
          env: DATABASE_URL
        SECRET_KEY:
          secret_name: my-secret    # Azure Key Vault secret
    
    run_job_retry:
      wait: 10              # Seconds between retries
      timeout: 300          # Total retry timeout
```

### Code Location Configuration Options

Each code location can be configured in three ways:

1. **Simple string configuration:**
   ```yaml
   my-code-location: my-container-group
   ```

2. **Environment variables and secrets:**
   ```yaml
   my-code-location:
     name: my-container-group
     subscription_id:
       secret_name: AZURE_SUBSCRIPTION_SECRET
     resource_group:
       env: RESOURCE_GROUP_VAR
     # Custom environment variables for the container
     DATABASE_URL:
       env: DATABASE_URL
     API_KEY:
       secret_name: api-key-secret
   ```

3. **Explicit values:**
   ```yaml
   my-code-location:
     name: my-container-group
     subscription_id: "12345678-1234-1234-1234-123456789012"
     resource_group: my-resource-group
     location: eastus
   ```

## Authentication

The launcher uses Azure's DefaultAzureCredential, which supports multiple authentication methods in order of precedence:

1. **Environment variables** (`AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`)
2. **Managed Identity** (when running on Azure resources)
3. **Azure CLI** (when authenticated via `az login`)
4. **Visual Studio Code** (when authenticated)
5. **Azure PowerShell** (when authenticated)

### Service Principal Authentication

For production deployments, use a service principal:

```bash
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret" 
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_SUBSCRIPTION_ID="your-subscription-id"
```

### Managed Identity

When running on Azure (VMs, Container Instances, etc.), managed identity is recommended:

```yaml
# No additional configuration needed - DefaultAzureCredential will use managed identity
```

## Required Azure Permissions

The service principal or managed identity needs the following permissions:

- `Microsoft.ContainerInstance/containerGroups/read`
- `Microsoft.ContainerInstance/containerGroups/write`
- `Microsoft.ContainerInstance/containerGroups/delete`
- `Microsoft.ContainerInstance/containerGroups/restart/action`

These are included in the built-in "Container Instance Contributor" role.

## Features

- **Isolated Execution**: Each run executes in its own container instance
- **Auto Scaling**: Container instances are created on-demand and terminated after completion
- **Multi-Location Support**: Different code locations can use different Azure configurations
- **Health Monitoring**: Built-in health checking for running containers
- **Graceful Termination**: Support for canceling runs and cleaning up resources
- **Retry Logic**: Configurable retry mechanisms for Azure API calls
- **Resource Limits**: Configurable CPU and memory limits per container

## Monitoring and Troubleshooting

### Viewing Container Logs

Use Azure CLI to view container logs:
```bash
az container logs --resource-group <resource-group> --name <container-group-name>
```

### Monitoring Resource Usage

View container metrics in the Azure portal or use Azure Monitor for detailed monitoring.

### Common Issues

1. **Authentication failures**: Ensure proper Azure credentials are configured
2. **Image pull failures**: Verify container registry access and image existence
3. **Resource quota limits**: Check Azure subscription limits for container instances
4. **Network connectivity**: Ensure container can access required resources

## Examples

See the `examples/` directory for complete configuration examples:
- `examples/dagster.yaml` - Complete instance configuration
- `examples/example_usage.py` - Programmatic usage example

## Limitations

- Container instances have a maximum runtime limit (depends on Azure region)
- Limited to Linux containers only
- No support for persistent storage (use external storage solutions)
- Container instances are billed per second while running

## See Also

- [Azure Container Instances Documentation](https://docs.microsoft.com/en-us/azure/container-instances/)
- [Dagster Run Launchers Documentation](https://docs.dagster.io/deployment/run-launcher)
- [Azure Authentication Documentation](https://docs.microsoft.com/en-us/python/api/overview/azure/identity-readme)
