import boto3
from boto3.dynamodb.conditions import Key, Attr


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

    #create_secondaryIndex()
    test_query()
    query_secondray_index()
