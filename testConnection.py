## Azure Cosmos SQL Core Sample 
##
## Purpose: Sample code for test our package 
##
## Author : Simon Li  Feb 2020
##
from simpleCosmosSQL import CosmosSQL
    
cosmos = CosmosSQL()
cosmos.createContainer(container_id = 'tasks', container_path = '/id')    

sql = 'SELECT t.id, t.title, t.description, t.done FROM {0} as t'.format(cosmos.container_id) 
for task in cosmos.queryItems(sql):
    print(task)