

from __future__ import print_function # Python 2/3 compatibility
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import decimal
import csv
from datetime import datetime
import argparse

import time

'''
thisdict = {
  "brand": "Ford",
  "model": "Mustang",
  "year": 1964
}
thisdict["color"] = "red"
print(thisdict)

'''



def create_MINMAX_duration(helper_table,Duration,KEY):
    global PrevKEY,LastRecord_tag,Helper_Table_dict
    KEY = KEY+"MINMAX"
    if Use_Local_Batch:
        items= Helper_Table_dict.get(KEY)
        if not items:
            print(KEY)
            Item={
                  'Key': KEY,
                  'Value': "["+Duration+"]["+Duration+"]",
                  'MIN_': int(Duration),
                  'MAX_': int(Duration),
                  'Exist': "Exist"
                  }
            Helper_Table_dict[KEY] = Item
            if PrevKEY:
                PrevItem = Helper_Table_dict[PrevKEY+"MINMAX"]
                helper_table.put_item(
                              Item={
                              'Key': PrevKEY+"MINMAX",
                              'Value': PrevItem["Value"],
                              'MIN_': PrevItem["MIN_"],
                              'MAX_': PrevItem["MAX_"],
                              'Exist': "Exist"
                              }
                              )
        else:
            
            MIN_ = items['MIN_']
            MAX_ = items['MAX_']
            dur = int(Duration)
            if dur>MAX_ or dur<MIN_:
                MIN_ = min(dur,MIN_)
                MAX_ = max(dur,MAX_)
                Item={
                      'Key': KEY,
                      'Value': "["+str(MIN_)+"]["+str(MAX_)+"]",
                      'MIN_': MIN_,
                      'MAX_': MAX_,
                      'Exist': "Exist"
                      }
                Helper_Table_dict[KEY] = Item
        if LastRecord_tag:
            CurItem = Helper_Table_dict[KEY]
            helper_table.put_item(
                          Item={
                          'Key': KEY,
                          'Value': CurItem["Value"],
                          'MIN_': CurItem["MIN_"],
                          'MAX_': CurItem["MAX_"],
                          'Exist': "Exist"
                          }
                          )
    else:               
        items = helper_table.query(KeyConditionExpression=Key('Key').eq(KEY))['Items']
        if not items:
            print(KEY)
            #this month not exists yet
            helper_table.put_item(
                                  Item={
                                  'Key': KEY,
                                  'Value': "["+Duration+"]["+Duration+"]",
                                  'MIN_': int(Duration),
                                  'MAX_': int(Duration),
                                  'Exist': "Exist"
                                  }
                                  )
        else:
            #this month already exists
            #print(items[0])
            #print(items[0]['Value'])
            MIN_ = items[0]['MIN_']
            MAX_ = items[0]['MAX_']
            dur = int(Duration)
            if dur>MAX_ or dur<MIN_:
                MIN_ = min(dur,MIN_)
                MAX_ = max(dur,MAX_)
                helper_table.update_item(
                                         Key={
                                         'Key': KEY
                                         },
                                         UpdateExpression="set MIN_=:min_, MAX_=:max_, Exist= Exist, #vl= :val",
                                         ExpressionAttributeValues={
                                         ':val': "["+str(MIN_)+"]["+str(MAX_)+"]",
                                         ':min_': MIN_,
                                         ':max_': MAX_
                                         },
                                         ExpressionAttributeNames={
                                         "#vl": "Value"
                                         },
                                         ReturnValues="UPDATED_NEW"
                                         )

def create_record_counts(helper_table,Duration,KEY,Cur_index):
    global PrevKEY,LastRecord_tag,Helper_Table_dict
    KEY = KEY+"RecordsCounts"
    if Use_Local_Batch:
        items= Helper_Table_dict.get(KEY)
        if not items:
                Item={
                      'Key': KEY,
                      'Value': "["+str(Cur_index)+"]&["+str(Cur_index)+"]",
                      'firstid': Cur_index,
                      'lastid': Cur_index,
                      'Exist': "Exist"
                      }
                Helper_Table_dict[KEY] = Item
                print("record{}".format(PrevKEY))
                if PrevKEY:
                    PrevItem = Helper_Table_dict[PrevKEY+"RecordsCounts"]
                    helper_table.put_item(
                                  Item={
                                  'Key': PrevKEY+"RecordsCounts",
                                  'Value': PrevItem["Value"],
                                  'firstid': PrevItem["firstid"],
                                  'lastid': PrevItem["lastid"],
                                  'Exist': "Exist"
                                  }
                                  )
                    helper_table.update_item(
                                             Key={
                                             'Key': 'LastLoadedIndex'
                                             },
                                             UpdateExpression="set NValue = :val",
                                             ExpressionAttributeValues={
                                             ':val': PrevItem["lastid"]
                                             },
                                             ReturnValues="UPDATED_NEW"
                                             )
                    print("loaded last id is {}\n".format(PrevItem["lastid"]))
        else:
            firstid = items['firstid']
            Item={
                  'Key': KEY,
                  'Value': "["+str(firstid)+"]["+str(Cur_index)+"]",
                  'firstid': firstid,
                  'lastid': Cur_index,
                  'Exist': "Exist"
                  }
            Helper_Table_dict[KEY] = Item
            
        if LastRecord_tag:
            CurItem = Helper_Table_dict[KEY]
            helper_table.put_item(
                          Item={
                          'Key': KEY,
                          'Value': CurItem["Value"],
                          'firstid': CurItem["firstid"],
                          'lastid': CurItem["lastid"],
                          'Exist': "Exist"
                          }
                          )
            helper_table.update_item(
                                     Key={
                                     'Key': 'LastLoadedIndex'
                                     },
                                     UpdateExpression="set NValue = :val",
                                     ExpressionAttributeValues={
                                     ':val': CurItem["lastid"]
                                     },
                                     ReturnValues="UPDATED_NEW"
                                     )
            print("loaded last id is {}\n".format(CurItem["lastid"]))
    else:
        items = helper_table.query(KeyConditionExpression=Key('Key').eq(KEY))['Items']
        if not items:
            #this month not exists yet
            helper_table.put_item(
                                  Item={
                                  'Key': KEY,
                                  'Value': "["+str(Cur_index)+"]&["+str(Cur_index)+"]",
                                  'firstid': Cur_index,
                                  'lastid': Cur_index,
                                  'Exist': "Exist"
                                  }
                                  )
        else:
            #this month already exists
            #print(items[0])
            firstid = items[0]['firstid']
            helper_table.update_item(
                                     Key={
                                     'Key': KEY
                                     },
                                     UpdateExpression="set firstid= firstid, lastid=:lid_, Exist= Exist, #vl= :val",
                                     ExpressionAttributeValues={
                                     ':val': "["+str(firstid)+"]&["+str(Cur_index)+"]",
                                     ':lid_': Cur_index
                                     },
                                     ExpressionAttributeNames={
                                     "#vl": "Value"
                                     },
                                     ReturnValues="UPDATED_NEW"
                                     )

def create_MembershipDistribution(helper_table,Duration,KEY,Membertype):
    global PrevKEY,LastRecord_tag,Helper_Table_dict
    ismember = False
    if Membertype=="Member":
        ismember = True
    KEY = KEY+"MembershipDistribution"
    if Use_Local_Batch:
        items= Helper_Table_dict.get(KEY)
        if not items:
            val = []
            mc = 0
            cc = 0
            if ismember:
                val = "[1]&[0]"
                mc = 1
            else:
                val = "[0]&[1]"
                cc = 1
            Item={
                  'Key': KEY,
                  'Value': val,
                  'MemberCounts': mc,
                  'CasualCounts': cc,
                  'Exist': "Exist"
                  }
            Helper_Table_dict[KEY] = Item
            if PrevKEY:
                PrevItem = Helper_Table_dict[PrevKEY+"MembershipDistribution"]
                helper_table.put_item(
                              Item={
                              'Key': PrevKEY+"MembershipDistribution",
                              'Value': PrevItem["Value"],
                              'MemberCounts': PrevItem["MemberCounts"],
                              'CasualCounts': PrevItem["CasualCounts"],
                              'Exist': "Exist"
                              }
                              )
        else:
            mc = items['MemberCounts']
            cc = items['CasualCounts']
            if ismember:
                mc = mc+1
            else:
                cc = cc+1
            Item={
                  'Key': KEY,
                  'Value': "["+str(mc)+"]&["+str(cc)+"]",
                  'MemberCounts': mc,
                  'CasualCounts': cc,
                  'Exist': "Exist"
                  }
            Helper_Table_dict[KEY] = Item
            
        if LastRecord_tag:
            CurItem = Helper_Table_dict[KEY]
            helper_table.put_item(
                          Item={
                          'Key': KEY,
                          'Value': CurItem["Value"],
                          'MemberCounts': CurItem["MemberCounts"],
                          'CasualCounts': CurItem["CasualCounts"],
                          'Exist': "Exist"
                          }
                          )
    else:
        
        items = helper_table.query(KeyConditionExpression=Key('Key').eq(KEY))['Items']
        if not items:
            #this month not exists yet
            val = []
            mc = 0
            cc = 0
            if ismember:
                val = "[1]&[0]"
                mc = 1
            else:
                val = "[0]&[1]"
                cc = 1
            helper_table.put_item(
                                  Item={
                                  'Key': KEY,
                                  'Value': val,
                                  'MemberCounts': mc,
                                  'CasualCounts': cc,
                                  'Exist': "Exist"
                                  }
                                  )
        else:
            #this month already exists
            #print(items[0])
            mc = items[0]['MemberCounts']
            cc = items[0]['CasualCounts']
            if ismember:
                mc = mc+1
            else:
                cc = cc+1
            helper_table.update_item(
                                     Key={
                                     'Key': KEY
                                     },
                                     UpdateExpression="set MemberCounts= :mc, CasualCounts=:cc, Exist= Exist, #vl= :val",
                                     ExpressionAttributeValues={
                                     ':val': "["+str(mc)+"]&["+str(cc)+"]",
                                     ':mc': mc,
                                     ':cc': cc
                                     },
                                     ExpressionAttributeNames={
                                     "#vl": "Value"
                                     },
                                     ReturnValues="UPDATED_NEW"
                                     )

def load_BikeSharing_record(helper_table,table,record,Cur_index):
    global PrevKEY,LastRecord_tag,Helper_Table_dict
    Index = Cur_index
    Duration = record['Duration']
    Startdate = record['Start date']
    Enddate = record['End date']
    Startstationnumber = record['Start station number']
    Startstation = record['Start station']
    Endstationnumber = record['End station number']
    Endstation = record['End station']
    Bikenumber = record['Bike number']
    Membertype = record['Member type']
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
    datetime_object = datetime.strptime(Startdate, '%Y-%m-%d %H:%M:%S')
    KEY = str(datetime_object.year)+"_"+str(datetime_object.month)+"_MIN_MAX"
    #print(KEY)
    create_MINMAX_duration(helper_table,Duration,KEY)
    create_record_counts(helper_table,Duration,KEY,Cur_index)
    create_MembershipDistribution(helper_table,Duration,KEY,Membertype)
    PrevKEY = KEY

def create_table(dynamodb,Table_Name):
    table = dynamodb.create_table(
                                  TableName=Table_Name,
                                  KeySchema=[
                                             {
                                             'AttributeName': 'Index',
                                             'KeyType': 'HASH'  #Partition key
                                             }
                                             ],
                                  AttributeDefinitions=[
                                                        {
                                                        'AttributeName': 'Index',
                                                        'AttributeType': 'N'
                                                        }
                                                        
                                                        ],
                                  ProvisionedThroughput={
                                  'ReadCapacityUnits': 10,
                                  'WriteCapacityUnits': 10
                                  }
                                  )
    
    while table.table_status=="CREATING":
        print("Table status:", table.table_status)
        time.sleep(1)
        table.reload()
    print("Table status:", table.table_status)

def create_and_Loadtable(Table_Name,File_name):
    global PrevKEY,LastRecord_tag,Helper_Table_dict,StoreOnline
    #Helper Table must exists, now we need to check if the table already been fully loaded or not
    if StoreOnline:
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        dynamodb_client = boto3.client('dynamodb',region_name='us-west-2')
    
    else:
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")
        dynamodb_client = boto3.client('dynamodb',region_name='us-west-2', endpoint_url="http://localhost:8000")
    
    existing_tables = dynamodb_client.list_tables()['TableNames']
    print(existing_tables)
    helper_table = dynamodb.Table(Table_Name+'HelperTable')
    
    Loaded_tag = Table_Name+"_Loaded"
    response = helper_table.get_item(
                              Key={
                              'Key': 'LastLoadedIndex'
                              }
                              )
    items = response['Item']
    print(items)
    Cur_index = items['NValue']
    print(Cur_index)
    
    
    if Table_Name not in existing_tables:
        #Table has not been created yet
        #Then we could assume no record have been imported
        create_table(dynamodb,Table_Name)
        Cur_index = Cur_index +1
        row_id = 1
        table = dynamodb.Table(Table_Name)
        print("Start loading data")
        num_records = 0
        with open(File_name) as csvFile:
            records = csv.DictReader(csvFile)
            num_records = len(list(records))
    
        with open(File_name) as csvFile:
            records = csv.DictReader(csvFile)
            print("rows number is {}".format(num_records))
            for record in records:
                if row_id == num_records:
                   LastRecord_tag = True
                load_BikeSharing_record(helper_table,table,record,Cur_index)
                if not Use_Local_Batch:
                    helper_table.update_item(
                                             Key={
                                             'Key': 'LastLoadedIndex'
                                             },
                                             UpdateExpression="set NValue = NValue + :val",
                                             ExpressionAttributeValues={
                                             ':val': decimal.Decimal(1)
                                             },
                                             ReturnValues="UPDATED_NEW"
                                             )
                if row_id ==1:
                    helper_table.put_item(
                                          Item={
                                          'Key': Table_Name+"FirstLoaded_index",
                                          'Value': "Exist",
                                          'NValue': Cur_index
                                          }
                                          )
                    print("write first id {}\n".format(Cur_index))
                    
                Cur_index = Cur_index+1
                row_id = row_id+1
                
                if row_id%1000 ==0:
                    print("Added record:", Cur_index)
    
        helper_table.put_item(
                                 Item={
                                 'Key': Loaded_tag,
                                 'Value':Loaded_tag
                                 }
                                 )
    else:
        #Table has not been created yet, we need to check if it has been fully loaded
        #assumption: Loaded tag been write to Helper Table only if a Table has been fully writed into dynamodb
        try:
            response = helper_table.get_item(
                                      Key={
                                      'Key': Loaded_tag,
                                      'Value':Loaded_tag
                                      }
                                      )
        
        except ClientError as e:
            #table not been fully loaded yet
            firstid = 0
            try:
                response = helper_table.get_item(
                                          Key={
                                          'Key': Table_Name+"FirstLoaded_index",
                                          'Value': "Exist"
                                          }
                                          )
            except ClientError:
                helper_table.put_item(
                                      Item={
                                      'Key': Table_Name+"FirstLoaded_index",
                                      'Value': "Exist",
                                      'NValue': Cur_index+1
                                      }
                                      )
                firstid = Cur_index
            else:
                firstid = response['Item']['NValue']
            print("firstid "+str(firstid))
            print("LastLoaded_index "+str(Cur_index))
            row_id = 1
            table = dynamodb.Table(Table_Name)
            num_records = 0
            with open(File_name) as csvFile:
                records = csv.DictReader(csvFile)
                num_records = len(list(records))
            with open(File_name) as csvFile:
                records = csv.DictReader(csvFile)
                for record in records:
                    if row_id == num_records:
                        LastRecord_tag = True
                    if firstid+row_id<Cur_index:
                        row_id = row_id+1
                    else:
                        load_BikeSharing_record(helper_table,table,record,Cur_index)
                        Cur_index = Cur_index+1
                        row_id = row_id+1
                        response = helper_table.update_item(
                                                            Key={
                                                            'Key': 'LastLoadedIndex'
                                                            },
                                                            UpdateExpression="set NValue = NValue + :val",
                                                            ExpressionAttributeValues={
                                                            ':val': decimal.Decimal(1)
                                                            },
                                                            ReturnValues="UPDATED_NEW"
                                                            )
                    if row_id%1000 ==0:
                        print("read record:", Cur_index)

            helper_table.put_item(
                                  Item={
                                  'Key': Loaded_tag,
                                  'Value':Loaded_tag
                                  }
                                  )

        else:
            #Table has already been fully loaded
            print("Table:"+Table_Name+"already been loaded, creation skipped\n")
    
    

'''
    {"Index":{"n":"1"},"Duration":{"n":"1012"},"Startdate":{"s":"2010-09-20 11:27:04"},"Enddate":{"s":"2010-09-20 11:43:56"},"Startstationnumber":{"n":"31208"},"Startstation":{"s":"M St & New Jersey Ave SE"},"Endstationnumber":{"n":"31108"},"Endstation":{"s":"4th & M St SW"},"Bikenumber":{"s":"742"},"Membertype":{"s":"Member"}}
    
'''


def test_query(TableName):
    '''
        s3 = boto3.resource('s3')
        for bucket in s3.buckets.all():
        print(bucket.name)
        '''
    # Get the service resource.#
    #dynamodb = boto3.resource('dynamodb')
    global StoreOnline
    if StoreOnline:
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    else:
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")
    # Instantiate a table resource object without actually
    # creating a DynamoDB table. Note that the attributes of this table
    # are lazy-loaded: a request is not made nor are the attribute
    # values populated until the attributes
    # on the table resource are accessed or its load() method is called.
    table = dynamodb.Table(TableName)
    
    # Print out some data about the table.
    # This will cause a request to be made to DynamoDB and its attribute
    # values will be set based on the response.
    #print(table.creation_date_time)
    
    
    response = table.query(KeyConditionExpression=Key('Index').eq(115597))
    items = response['Items']
    print(items)


def create_secondaryIndex(TableName):
    # Boto3 is the AWS SDK library for Python.
    # You can use the low-level client to make API calls to DynamoDB.
    global StoreOnline
    if StoreOnline:
        client = boto3.client('dynamodb', region_name='us-west-2')
    else:
        client = boto3.client('dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")
    
    try:
        resp = client.update_table(
                                   TableName=TableName,
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
        resp = client.update_table(
                                   TableName=TableName,
                                   # Any attributes used in your new global secondary index must be declared in AttributeDefinitions
                                   AttributeDefinitions=[
                                                         {
                                                         "AttributeName": "Startstationnumber",
                                                         "AttributeType": "S"
                                                         },
                                                         ],
                                   # This is where you add, update, or delete any global secondary indexes on your table.
                                   GlobalSecondaryIndexUpdates=[
                                                                {
                                                                "Create": {
                                                                # You need to name your index and specifically refer to it when using it for queries.
                                                                "IndexName": "Startstationnumber",
                                                                # Like the table itself, you need to specify the key schema for an index.
                                                                # For a global secondary index, you can use a simple or composite key schema.
                                                                "KeySchema": [
                                                                              {
                                                                              "AttributeName": "Startstationnumber",
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
        resp = client.update_table(
                                   TableName=TableName,
                                   # Any attributes used in your new global secondary index must be declared in AttributeDefinitions
                                   AttributeDefinitions=[
                                                         {
                                                         "AttributeName": "Endstationnumber",
                                                         "AttributeType": "S"
                                                         },
                                                         ],
                                   # This is where you add, update, or delete any global secondary indexes on your table.
                                   GlobalSecondaryIndexUpdates=[
                                                                {
                                                                "Create": {
                                                                # You need to name your index and specifically refer to it when using it for queries.
                                                                "IndexName": "Endstationnumber",
                                                                # Like the table itself, you need to specify the key schema for an index.
                                                                # For a global secondary index, you can use a simple or composite key schema.
                                                                "KeySchema": [
                                                                              {
                                                                              "AttributeName": "Endstationnumber",
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

def query_secondray_index(TableName):
    # Boto3 is the AWS SDK library for Python.
    # The "resources" interface allows for a higher-level abstraction than the low-level client interface.
    # For more details, go to http://boto3.readthedocs.io/en/latest/guide/resources.html
    global StoreOnline
    if StoreOnline:
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    else:
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")
    table = dynamodb.Table(TableName)
    
    # When adding a global secondary index to an existing table, you cannot query the index until it has been backfilled.
    # This portion of the script waits until the index is in the “ACTIVE” status, indicating it is ready to be queried.
    while True:
        if not table.global_secondary_indexes or table.global_secondary_indexes[0]['IndexStatus'] != 'ACTIVE':
            print('Waiting for index to backfill...')
            time.sleep(1)
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
        
def create_HelperTable(Table_Name):
    #create helper table if HelperTable not exists
    global StoreOnline
    if StoreOnline:
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        dynamodb_client = boto3.client('dynamodb',region_name='us-west-2')
    else:
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")
        dynamodb_client = boto3.client('dynamodb',region_name='us-west-2', endpoint_url="http://localhost:8000")
    
    table_name = Table_Name+'HelperTable'
    existing_tables = dynamodb_client.list_tables()['TableNames']
    if table_name not in existing_tables:
        response = dynamodb_client.create_table(
                                                AttributeDefinitions=[
                                                                      {
                                                                      'AttributeName': 'Key',
                                                                      'AttributeType': 'S',
                                                                      }
                                                                      ],
                                                KeySchema=[
                                                           {
                                                           'AttributeName': 'Key',
                                                           'KeyType': 'HASH',
                                                           }
                                                           ],
                                                ProvisionedThroughput={
                                                'ReadCapacityUnits': 10,
                                                'WriteCapacityUnits': 10,
                                                },
                                                TableName=table_name
                                                )
        helper_table = dynamodb.Table(table_name)
        while helper_table.table_status=="CREATING":
            print("Table status:", helper_table.table_status)
            time.sleep(1)
            helper_table.reload()
        print("Table status:", helper_table.table_status)
        helper_table.put_item(
                       Item={
                       'Key': 'LastLoadedIndex',
                       'NValue': 0
                       }
                       )
        print("Helper Table been created\n")
    else:
        print("Helper Table already been created\n")

if __name__=="__main__":
    
    global PrevKEY,LastRecord_tag,Helper_Table_dict,Use_Local_Batch,StoreOnline
    Use_Local_Batch = True
    PrevKEY = [];
    LastRecord_tag = False;
    Helper_Table_dict = {}
    parser = argparse.ArgumentParser(description='-r to reset,-b to enable batch process')
    parser.add_argument('-r', action='store_true')
    parser.add_argument('-b', action='store_true')
    parser.add_argument('-o', action='store_true')
    args = parser.parse_args()
    print(args)
 
    RESET = args.r
    Use_Local_Batch = args.b
    StoreOnline = args.o
    print("RESET{}".format(RESET))
    print("Use_Local_Batch{}".format(Use_Local_Batch))
    print("StoreOnline{}".format(StoreOnline))
    if RESET:
        if StoreOnline:
            dynamodb_client = boto3.client('dynamodb',region_name='us-west-2')
        else:
            dynamodb_client = boto3.client('dynamodb',region_name='us-west-2', endpoint_url="http://localhost:8000")
        dynamodb_client.delete_table(TableName="Bikesharing2010_batchHelperTable")
        dynamodb_client.delete_table(TableName="Bikesharing2010_batch")
    
    
    File_name = "2010-capitalbikeshare-tripdata.csv"
    Table_Name = 'Bikesharing2010_batch'
    create_HelperTable(Table_Name)
    create_and_Loadtable(Table_Name,File_name)
    #create_secondaryIndex(Table_Name)
    #test_query(TableName)
    #query_secondray_index(TableName)

