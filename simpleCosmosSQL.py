## Azure Cosmos SQL Core Sample 
##
## Purpose: Sample code for Azure Cosmos SQL 
##
## Author : Simon Li  Feb 2020
##
## https://pypi.org/project/azure-cosmos/
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.http_constants as http_constants
import azure.cosmos.documents as documents

import config as cfg

class CosmosSQL: 
    def __init__(self, database_id = 'testDatabase'):
        # Create client
        self.__client = cosmos_client.CosmosClient(cfg.settings['URI'], {'masterKey': cfg.settings['PRIMARY_KEY']})
        self.__database_id = database_id
        self.createDatabase_if_not_exists(database_id)

    def createDatabase_if_not_exists(self, database_id): 
        # Create a database
        try:
            self.__database = self.client.CreateDatabase({'id': database_id})
        except errors.HTTPFailure:
            self.__database = self.client.ReadDatabase("dbs/" + database_id)

    def __enter__(self):
        return (self.__client, self.__database) # bound to target

    def __exit__(self, exception_type, exception_val, trace):
        # extra cleanup in here
        self.client.CloseDatabase({"id": self.__database})
        self.client = None

    @property
    def client(self):
        return self.__client 

    @property
    def database(self):
        return self.__database 

    @database.setter
    def database(self, database_id):
        self.__database_id = database_id
        self.createDatabase_if_not_exists(database_id)

    @property
    def database_id(self):
        return self.__database_id 

    @property
    def container(self):
        return self.__container 
    
    @container.setter
    def container(self, container_id, container_path):
        self.createContainer(container_id, container_path) 

    @property
    def container_id(self):
        return self.__container_id 

    # Create a container    
    def createContainer(self, container_id, container_path): 
        self.__container_id = container_id   
        container_definition = {'id': container_id,
                                'partitionKey':
                                            {
                                                'paths': [container_path],
                                                'kind': documents.PartitionKind.Hash
                                            }
                                }
        try:
            self.__container = self.client.CreateContainer("dbs/" + self.database['id'], container_definition, {'offerThroughput': 400})
        except errors.HTTPFailure as e:
            if e.status_code == http_constants.StatusCodes.CONFLICT:
                self.__container = self.client.ReadContainer("dbs/" + self.database['id'] + "/colls/" + container_definition['id'])
            else:
                raise e
    
    def readContainer(self, container_id):
        self.__container_id = container_id
        self.__container = self.client.ReadContainer("dbs/" + self.database_id + "/colls/" + container_id)
        return self.__container

    def replaceContainer(self, container):
        self.__container = self.client.ReplaceContainer("dbs/" + self.database_id + "/colls/" + self.container_id, container)
        return self.__container

    # Replace throughput for a container
    def replaceThroughputOfContainer(self, value = 1000):            
        # Get the offer for the container
        container = self.container
        offers = list(self.client.QueryOffers("Select * from root r where r.offerResourceId='" + container['_rid'] + "'"))
        offer = offers[0]
        print("current throughput for " + container['id'] + ": " + str(offer['content']['offerThroughput']))

        # Replace the offer with a new throughput
        if value != offer['content']['offerThroughput']: 
            offer['content']['offerThroughput'] = value
            self.client.ReplaceOffer(offer['_self'], offer)
            print("new throughput for " + container['id'] + ": " + str(offer['content']['offerThroughput']))

    # Get an existing container
    def getContainer(self, container_id):
        return self.client.ReadContainer("dbs/" + self.database_id + "/colls/" + container_id)

    # Collection operations
    def upsertItem(self, fields):
        self.client.UpsertItem("dbs/" + self.database_id + "/colls/" + self.container_id, fields)

    def queryItems(self, sql):
        return self.client.QueryItems("dbs/" + self.database_id + "/colls/" + self.container_id, sql, {'enableCrossPartitionQuery': True})

    def deleteItem(self, itemId):
        self.client.DeleteItem("dbs/" + self.database_id + "/colls/" + self.container_id + "/docs/" + itemId, {'partitionKey': 'Pager'})

if __name__ == '__main__':
    cosmos = CosmosSQL()

    cosmos.createContainer(container_id = 'products', container_path = '/productName')
    cosmos.replaceThroughputOfContainer(1000) 

    # Insert data
    for i in range(1, 10):
        cosmos.upsertItem({
            'id': 'item{0}'.format(i),
            'productName': 'Widget',
            'productModel': 'Model {0}'.format(i)
        }
    )

    # Delete data
    sql = 'SELECT * FROM ' + cosmos.container_id + ' p WHERE p.productModel = "DISCONTINUED"'
    for item in cosmos.queryItems(sql):
        cosmos.deleteItem(item['id'])

    # Query the database
    database = cosmos.database
    container = cosmos.container

    # Enumerate the returned items
    import json
    sql = 'SELECT * FROM ' + cosmos.container_id + ' r WHERE r.id="item3"'
    for item in cosmos.queryItems(sql):
        print(json.dumps(item, indent=True))

    discontinued_items = cosmos.queryItems({
                                            'query': 'SELECT * FROM root r WHERE r.id=@id',
                                            'parameters': [
                                                {'name': '@id', 'value': 'item4'}
                                            ]
                                           })
                                       
    for item in discontinued_items:
        print(json.dumps(item, indent=True))

    # Modify container properties
    container = cosmos.container
    container['defaultTtl'] = 10
    modified_container = cosmos.replaceContainer(container)
    
    # Display the new TTL setting for the container
    print(json.dumps(modified_container['defaultTtl']))
