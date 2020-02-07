## Azure Cosmos SQL Core Sample 
##
## Purpose: Sample code for test our package 
##
## Author : Simon Li  Feb 2020
##
from simpleCosmosSQL import CosmosSQL

import json

def initialization(cosmos):
    # Test Dataset
    tasks = [
            {
                'id': '1',
                'title': u'Buy groceries',
                'description': u'Milk, Cheese, Pizza, Fruit, Tylenol', 
                'done': False
            },
            {
                'id': '2',
                'title': u'Learn Python',
                'description': u'Need to find a good Python tutorial on the web', 
                'done': False
            },
            {
                "id": '3',
                "title": u"Use flask",
                "description": u"Use flask to build RESTful service",
                "done": True
            } 
        ]           
    for task in tasks:        
        cosmos.upsertItem(task)

def query(cosmos):
# Query data
    sql = 'SELECT t.id, t.title, t.description FROM {0} as t'.format(cosmos.container_id) 
    for task in cosmos.queryItems(sql):
        print(json.dumps(task, indent=True))

def delete(cosmos, id):
    # Enumerate the returned items
    return cosmos.deleteItem(id)

def put(cosmos, document):
    return cosmos.upsertItem(document)

def patch(cosmos, id, document):
    return cosmos.patchItem(id, document)

def read(cosmos, id):
    # Enumerate the returned items
    tasks = cosmos.queryItems({
                                'query': 'SELECT r.id, r.title FROM root r WHERE r.id=@id',
                                'parameters': [
                                                {'name': '@id', 'value': id}
                                            ]
                            })                                       
    for task in tasks:
        print(json.dumps(task, indent=True))
    return tasks

cosmos = CosmosSQL()
cosmos.recreateContainer(container_id = 'tasks', container_path = '/id')    

print("\n==============================================")
print("**Test Insert")
initialization(cosmos)
query(cosmos)

print("\n==============================================")
print("**Test Patch")
result = cosmos.patchItem('2', {'title': 'This is a patch'})
print(result)

print("\n==============================================")
print("**Test Put")
result = put(cosmos, {
                "id": '3',
                "title": u"Upadte flask",
                "description": u"Update flask to build RESTful service",
                "done": True
                }
        )
print(result)
#query(cosmos)

print("\n==============================================")
print("**Test Read")
result = read(cosmos, '2')
print(result)

print("\n==============================================")
print("**Test Delete")
result = delete(cosmos, '3')
print(result)

print("\n==============================================")
query(cosmos)