## Azure Cosmos SQL Core Sample 
##
## Purpose: Sample code for test our package 
##
## Author : Simon Li  Feb 2020
##
import uuid
import json

class family:
    @staticmethod
    def get_andersen_family_item():
        andersen_item = {
            'id': 'Andersen_' + str(uuid.uuid4()),
            'lastName': 'Andersen',
            'district': 'WA5',
            'parents': [
                {
                    'familyName': None,
                    'firstName': 'Thomas'
                },
                {
                    'familyName': None,
                    'firstName': 'Mary Kay'
                }
            ],
            'children': None,
            'address': {
                'state': 'WA',
                'county': 'King',
                'city': 'Seattle'
            },
            'registered': True
        }
        return andersen_item

    @staticmethod
    def get_wakefield_family_item():
        wakefield_item = {
            'id': 'Wakefield_' + str(uuid.uuid4()),
            'lastName': 'Wakefield',
            'district': 'NY23',
            'parents': [
                {
                    'familyName': 'Wakefield',
                    'firstName': 'Robin'
                },
                {
                    'familyName': 'Miller',
                    'firstName': 'Ben'
                }
            ],
            'children': [
                {
                    'familyName': 'Merriam',
                    'firstName': 'Jesse',
                    'gender': None,
                    'grade': 8,
                    'pets': [
                        {
                            'givenName': 'Goofy'
                        },
                        {
                            'givenName': 'Shadow'
                        }
                    ]
                },
                {
                    'familyName': 'Miller',
                    'firstName': 'Lisa',
                    'gender': 'female',
                    'grade': 1,
                    'pets': None
                }
            ],
            'address': {
                'state': 'NY',
                'county': 'Manhattan',
                'city': 'NY'
            },
            'registered': True
        }
        return wakefield_item

    @staticmethod
    def get_smith_family_item():
        smith_item = {
            'id': 'Johnson_' + str(uuid.uuid4()),
            'lastName': 'Johnson',
            'district': None,
            'registered': False
        }
        return smith_item

    @staticmethod
    def get_johnson_family_item():
        johnson_item = {
            'id': 'Smith_' + str(uuid.uuid4()),
            'lastName': 'Smith',
            'parents': None,
            'children': None,
            'address': {
                'state': 'WA',
                'city': 'Redmond'
            },   
            'registered': True
        }
        return johnson_item

from CosmosSQLService import CosmosSQL

cosmos = CosmosSQL()

# Create a container
# Using a good partition key improves the performance of database operations.
cosmos.recreateContainer(container_id = 'FamilyContainer', container_path = '/lastName')  

# Add items to the container
family_items_to_create = [family.get_andersen_family_item(), family.get_johnson_family_item(), family.get_smith_family_item(), family.get_wakefield_family_item()]
print('Number of families: {0}.'.format(len(family_items_to_create)))

 # <create_item>
for family_item in family_items_to_create:
    cosmos.upsertItem(family_item)
# </create_item>

# Read items (key value lookups by partition key and id, aka point reads)
# <read_item>
for family in family_items_to_create:
    item_response = cosmos.readItem(itemId = family['id'])
    print('Read item with id {0}, lastname {1}.'.format(item_response['id'], item_response['lastName']))
# </read_item>

# Query these items using the SQL query syntax. 
# Specifying the partition key value in the query allows Cosmos DB to retrieve data only from the relevant partitions, which improves performance
# <query_items>
query = "SELECT * FROM c WHERE c.lastName IN ('Wakefield', 'Andersen')"
items = cosmos.queryItems(query)
count = 0
for item in items:
    count += 1
    print(json.dumps(item, indent=True))

print('Query returned {0} items.'.format(count))
# </query_items>
