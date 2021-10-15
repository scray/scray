# Create Kubernetes cluster in Microsoft Azure cloud

## Deploy - portal
[![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Fscray%2Fscray%2Ffeature%2Fcloud-env%2Fprojects%2Finvoice-hyperledger-fabric%2Fdeployment-environments%2Fmicrosoft-azure%2Faks-template.json)

## Deploy - CLI
* Install [Azure CLI](https://docs.microsoft.com/de-de/cli/azure/install-azure-cli)
* Open Windows PowerShell
* Login
  ```
  az login
  ```
* Create resource group and apply template
  ```
  az group create --name Scray-Blockchain-Test --location "eastus"
  az deployment group create   --name Scray-Blockchain-Test   --resource-group Scray-Blockchain-Test   --template-file aks-template.json
  ```

## Access cluster 
* Get kubernetes credentials
  ```
  az aks get-credentials --overwrite-existing  --resource-group Scray-Blockchain-Test --name Scray-Blockchain-Test
  ```