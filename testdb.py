## Azure Cosmos SQL Core Sample 
##
## Purpose: Sample code for test our package 
##
## Author : Simon Li  Feb 2020
##
from CosmosSQLService import CosmosSQLClient

client = CosmosSQLClient()
database_id = 'testDatabase'

try:                                    
    # query for a database        
    client.findDatabase(database_id)

    # get a database using its id
    client.readDatabase(database_id)

    # list all databases on an account
    client.listDatabases()

except CosmosSQLClient.errors.HTTPFailure as e:
    print('\ntestdb has caught an error. {0}'.format(e))
        
finally:
    print("\ntestd done")