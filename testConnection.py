## Azure Cosmos SQL Core Sample 
##
## Purpose: Sample code for test our package 
##
## Author : Simon Li  Feb 2020
##
from CosmosSQLService import CosmosSQLClient

# Create a client    
cosmosClient = CosmosSQLClient()
# List all the databases under our account
cosmosClient.listDatabases()