# Create Kubernetes cluster
* Install Azure CLI
* Open Windows PowerShell
  ```
  az login
  ```


## Install Template

Create resource group and apply template
```
az group create --name Scray-Blockchain-Test --location "eastus"
az deployment group create   --name Scray-Blockchain-Test   --resource-group Scray-Blockchain-Test   --template-file aks-template.json

```

Get kubernetes credentials
```aidl
az aks get-credentials --overwrite-existing  --resource-group Scray-Blockchain-Test --name Scray-Blockchain-Test
```

## Uninstall Template

```
az deployment group delete   --name ExampleDeployment   --resource-group Scray-Blockchain-Test
```