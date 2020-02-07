## Azure Cosmos SQL Core Sample 
##
## Purpose: Sample code for Azure Cosmos SQL 
##
## Author : Simon Li  Feb 2020
##
##############################################################################################
## ref: https://pypi.org/project/azure-cosmos/
## Usage:
## 1. Import our library
##    from simpleCosmosSQL import CosmosSQL
##
## 2. Create an instance with a database, which will be created if not existing
##    cosmos = CosmosSQL('myDatabase')      
## 
## 3. Create a container/collection if it doesn't exist
##    cosmos.createContainer('collection_name', '/fieldname')
##############################################################################################
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.http_constants as http_constants
import azure.cosmos.documents as documents

import uuid
import json

import config as cfg

#https://docs.microsoft.com/en-us/python/api/azure-cosmos/azure.cosmos.cosmos_client.cosmosclient?view=azure-python
# Class CosmosSQLClient is served for client and database
class CosmosSQLClient: 
    def __init__(self):
        self.__client = cosmos_client.CosmosClient(cfg.settings['URI'], {'masterKey': cfg.settings['PRIMARY_KEY']})

    def __enter__(self):
        return self.__client # bound to target

    def __exit__(self, exception_type, exception_val, trace):
        # extra cleanup in here
        self.client = None

    def getClient(self):
        return self.__client 

    @property
    def client(self):
        return self.__client 

    def createDatabaseIfNotExists(self, database_id): 
        # Create a database
        try:
            database = self.client.CreateDatabase({'id': database_id})
        except errors.HTTPFailure:
            database = self.client.ReadDatabase("dbs/" + database_id)
        return database

    def findDatabase(self, id):
        print('Query for Database')
        databases = list(self.client.QueryDatabases({
            "query": "SELECT * FROM r WHERE r.id=@id",
            "parameters": [
                { "name":"@id", "value": id }
            ]
        }))
        if len(databases) > 0:
            print('Database with id \'{0}\' was found'.format(id))
        else:
            print('No database with id \'{0}\' was found'. format(id))

    def readDatabase(self, id):
        print("\nGet a Database by id")
        try:
            # All Azure Cosmos resources are addressable via a link
            # This link is constructed from a combination of resource hierachy and 
            # the resource id. 
            # Eg. The link for database with an id of Foo would be dbs/Foo
            database_link = 'dbs/' + id

            database = self.client.ReadDatabase(database_link)
            print('Database with id \'{0}\' was found, it\'s _self is {1}'.format(id, database['_self']))

        except errors.HTTPFailure as e:
            if e.status_code == 404:
                print('A database with id \'{0}\' does not exist'.format(id))
            else: 
                raise

    def listDatabases(self):
        print("\nList all Databases on an account")
        print('Databases:')
        for database in list(self.client.ReadDatabases()):
            print(database['id'])     

    #Expose the libraries
    @staticmethod
    def errors():
        return errors

    @staticmethod
    def json():
        return json

    @staticmethod
    def uuid():
        return uuid

###################################################################################        
# Class CosmosSQL main service
class CosmosSQL(CosmosSQLClient): 
    def __init__(self, database_id = 'testDatabase'):
        # Create client
        super().__init__()
        
        self.__database_id = database_id
        self.__database = self.createDatabaseIfNotExists(database_id)

    def __enter__(self):
        return (self.client, self.__database) # bound to target

    def __exit__(self, exception_type, exception_val, trace):
        # extra cleanup in here
        self.client.CloseDatabase({"id": self.__database})
        super.__exit__(exception_type, exception_val, trace)

    @property
    def client(self):
        return self.getClient() 

    @property
    def database(self):
        return self.__database 

    @database.setter
    def database(self, database_id):
        self.__database_id = database_id
        self.createDatabaseIfNotExists(database_id)

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
    def createContainer(self, container_id, container_path = '/id'): 
        self.__container_id = container_id   
        container_definition = {'id': container_id}
        container_definition['partitionKey'] = {
                                                    'paths': [container_path],
                                                    'kind': documents.PartitionKind.Hash
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

    def deleteContainer(self, container_id):
        if not container_id:
            container_id = self.container_id
        try:
            self.__container = self.client.DeleteContainer("dbs/" + self.database_id + "/colls/" + container_id)
        except errors.HTTPFailure: # as e:
            print("container_id {0} does not exist".format(container_id))

    def recreateContainer(self, container_id, container_path = '/id'): 
        self.deleteContainer(container_id)
        self.createContainer(container_id, container_path)

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
    def upsertItem(self, document):
        return self.client.UpsertItem("dbs/" + self.database_id + "/colls/" + self.container_id, document)

    def patchItem(self, id, partialDoc):
        if 'id' in partialDoc: 
            del partialDoc['id']
        sql = 'SELECT * FROM ' + self.container_id + " t WHERE t.id = '{0}'".format(id)
        for item in self.queryItems(sql):
            document = {}
            for key in item.keys():
                if key[0] != '_':
                    document[key] = item[key]
            document.update(partialDoc) 
            return self.upsertItem(document)

    def queryItems(self, sql = ""):
        if sql == "":
            sql = 'SELECT * FROM ' + self.container_id   
        return self.client.QueryItems("dbs/" + self.database_id + "/colls/" + self.container_id, sql, {'enableCrossPartitionQuery': True})

    def listItems(self):
        for item in self.queryItems():
            print(item)

    def listItemsJson(self):
        for item in self.queryItems():
            print(json.dumps(item, indent=True))

    def readItem(self, itemId):
        items = self.queryItems({
                                    'query': 'SELECT * FROM root r WHERE r.id=@id',
                                    'parameters': [
                                            {'name': '@id', 'value': itemId}
                                    ]
                                })
        document = {}
        for item in items:
            for key in item.keys():
                if key[0] != '_':
                    document[key] = item[key]
            document = item
            break
        
        return document

    def deleteItem(self, itemId, partitionKey = None):
        if not partitionKey:
            partitionKey = itemId
        options = {'enableCrossPartitionQuery': True}
        options['maxItemCount'] = 5
        options['partitionKey'] = partitionKey
        return self.client.DeleteItem("dbs/" + self.database_id + "/colls/" + self.container_id + "/docs/" + itemId , options)

    #Expose id function
    @property
    def id(self):
        return str(uuid.uuid4())

###################################################################################        
# Test Section
if __name__ == '__main__':
    cosmos = CosmosSQL()

    cosmos.recreateContainer(container_id = 'products', container_path = '/productName')
    
    cosmos.replaceThroughputOfContainer(1000) 

    # Insert data
    for i in range(1, 10):
        cosmos.upsertItem({
            'id': cosmos.id,
            'itemId': 'item{0}'.format(i),
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
    sql = 'SELECT * FROM ' + cosmos.container_id + ' r WHERE r.itemId="item3"'
    for item in cosmos.queryItems(sql):
        print(json.dumps(item, indent=True))

    discontinued_items = cosmos.queryItems({
                                            'query': 'SELECT * FROM root r WHERE r.itemId=@id',
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
