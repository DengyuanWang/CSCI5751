

from __future__ import print_function # Python 2/3 compatibility
import boto3
from boto3.dynamodb.conditions import Key, Attr

import decimal
import csv
def create_table_():
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")

    table = dynamodb.create_table(
                                  TableName='Bikesharing2010',
                                  KeySchema=[
                                             {
                                             'AttributeName': 'Index',
                                             'KeyType': 'HASH'  #Partition key
                                             },
                                             {
                                             'AttributeName': 'Startdate',
                                             'KeyType': 'String'  #Sort key
                                             }
                                             ],
                                  AttributeDefinitions=[
                                                        {
                                                        'AttributeName': 'Index',
                                                        'AttributeType': 'N'
                                                        },
                                                        {
                                                        'AttributeName': 'Startdate',
                                                        'AttributeType': 'S'
                                                        },
                                                        
                                                        ],
                                  ProvisionedThroughput={
                                  'ReadCapacityUnits': 10,
                                  'WriteCapacityUnits': 10
                                  }
                                  )


    print("Table status:", table.table_status)

    table = dynamodb.Table('Bikesharing2010')


    Cur_index = 1
    with open("2010-capitalbikeshare-tripdata.csv") as csvFile:
        records = csv.DictReader(csvFile)
        for record in records:
            Index = Cur_index
            Cur_index = Cur_index+1
            Duration = record['Duration']
            Startdate = record['Start date']
            Enddate = record['End date']
            Startstationnumber = record['Start station number']
            Startstation = record['Start station']
            Endstationnumber = record['End station number']
            Endstation = record['End station']
            Bikenumber = record['Bike number']
            Membertype = record['Member type']
            if Index%5000 ==0:
                print("Added record:", Index)
            
            table.put_item(
                           Item={
                           'Index': Index,
                           'Startdate': Startdate,
                           'Duration': Duration,
                           'Enddate': Enddate,
                           'Startstationnumber': Startstationnumber,
                           'Startstation': Startstation,
                           'Endstationnumber': Endstationnumber,
                           'Endstation': Endstation,
                           'Bikenumber': Bikenumber,
                           'Membertype': Membertype,
                           }
                           )
'''
    {"Index":{"n":"1"},"Duration":{"n":"1012"},"Startdate":{"s":"2010-09-20 11:27:04"},"Enddate":{"s":"2010-09-20 11:43:56"},"Startstationnumber":{"n":"31208"},"Startstation":{"s":"M St & New Jersey Ave SE"},"Endstationnumber":{"n":"31108"},"Endstation":{"s":"4th & M St SW"},"Bikenumber":{"s":"742"},"Membertype":{"s":"Member"}}
    
'''



def test_query():
    '''
        s3 = boto3.resource('s3')
        for bucket in s3.buckets.all():
        print(bucket.name)
        '''
    # Get the service resource.#
    #dynamodb = boto3.resource('dynamodb')
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")
    # Instantiate a table resource object without actually
    # creating a DynamoDB table. Note that the attributes of this table
    # are lazy-loaded: a request is not made nor are the attribute
    # values populated until the attributes
    # on the table resource are accessed or its load() method is called.
    table = dynamodb.Table('Bikesharing2010')
    
    # Print out some data about the table.
    # This will cause a request to be made to DynamoDB and its attribute
    # values will be set based on the response.
    #print(table.creation_date_time)
    
    
    response = table.query(KeyConditionExpression=Key('Index').eq(115597))
    items = response['Items']
    print(items)


def create_secondaryIndex():
    # Boto3 is the AWS SDK library for Python.
    # You can use the low-level client to make API calls to DynamoDB.
    client = boto3.client('dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")
    
    try:
        resp = client.update_table(
                                   TableName="Bikesharing2010",
                                   # Any attributes used in your new global secondary index must be declared in AttributeDefinitions
                                   AttributeDefinitions=[
                                                         {
                                                         "AttributeName": "Bikenumber",
                                                         "AttributeType": "S"
                                                         },
                                                         ],
                                   # This is where you add, update, or delete any global secondary indexes on your table.
                                   GlobalSecondaryIndexUpdates=[
                                                                {
                                                                "Create": {
                                                                # You need to name your index and specifically refer to it when using it for queries.
                                                                "IndexName": "BikenumberIndex",
                                                                # Like the table itself, you need to specify the key schema for an index.
                                                                # For a global secondary index, you can use a simple or composite key schema.
                                                                "KeySchema": [
                                                                              {
                                                                              "AttributeName": "Bikenumber",
                                                                              "KeyType": "HASH"
                                                                              }
                                                                              ],
                                                                # You can choose to copy only specific attributes from the original item into the index.
                                                                # You might want to copy only a few attributes to save space.
                                                                "Projection": {
                                                                "ProjectionType": "ALL"
                                                                },
                                                                # Global secondary indexes have read and write capacity separate from the underlying table.
                                                                "ProvisionedThroughput": {
                                                                "ReadCapacityUnits": 5,
                                                                "WriteCapacityUnits": 5,
                                                                }
                                                                }
                                                                }
                                                                ],
                                   )
        print("Secondary index added!")
    except Exception as e:
        print("Error updating table:")
        print(e)

def query_secondray_index():
    # Boto3 is the AWS SDK library for Python.
    # The "resources" interface allows for a higher-level abstraction than the low-level client interface.
    # For more details, go to http://boto3.readthedocs.io/en/latest/guide/resources.html
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")
    table = dynamodb.Table('Bikesharing2010')
    
    # When adding a global secondary index to an existing table, you cannot query the index until it has been backfilled.
    # This portion of the script waits until the index is in the “ACTIVE” status, indicating it is ready to be queried.
    while True:
        if not table.global_secondary_indexes or table.global_secondary_indexes[0]['IndexStatus'] != 'ACTIVE':
            print('Waiting for index to backfill...')
            time.sleep(5)
            table.reload()
        else:
            break

# When making a Query call, you use the KeyConditionExpression parameter to specify the hash key on which you want to query.
# If you want to use a specific index, you also need to pass the IndexName in our API call.
resp = table.query(
                   # Add the name of the index you want to use in your query.
                   IndexName="BikenumberIndex",
                   KeyConditionExpression=Key('Bikenumber').eq('W00771'),
                   )
    
    print("The query returned the following items:")
    for item in resp['Items']:
        print(item)

if __name__=="__main__":
    #create_table_()
    #create_secondaryIndex()
    test_query()
    query_secondray_index()

