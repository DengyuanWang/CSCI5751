import boto3

# Get the service resource.
dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('BikeSharing2010')

startIndex = 0
left = 1
right = table.item_count
mid = left + (right-left)/2
while right-left>1:
    response = table.get_item(
        Key={
            'Index': mid
        }
    )
    date = response['Item']['Startdate'][5:10]
    dates = (int)(date[0:2])*100+(int)(date[3:5])
    if dates==1001:
        break
    elif dates<1001:
        left = mid
    else:
        right = mid
    mid = left+(right-left)/2

while True:
    response = table.get_item(
        Key={
            'Index': mid
        }
    )
    if mid<=0:
        break
    elif response['Item']['Startdate'][5:10]=='10-01':
        mid = mid-1
    else:
        break

while True:
    mid = mid+1
    response = table.get_item(
        Key={
            'Index': mid
        }
    )
    if mid>table.item_count:
        break
    elif response['Item']['Startdate'][5:10]=='10-01':
        print(response['Item'])
    else:
        break
