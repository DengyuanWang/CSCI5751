import boto3
import sys

def get_min_temp(start_year, start_month, end_year, end_month):

    dynamodb = boto3.resource('dynamodb')

    if start_year>end_year or (start_year==end_year and start_month>end_month) or start_year<2010 or end_year>2020 or start_month<0 or start_month>12:
        print("timestamp out of range!")
        return
    else:
        res = sys.float_info.max
        for year in range(start_year, end_year+1):
            tableName = "Bikesharing"+str(year)+"_batchHelperTable"
            table = dynamodb.Table(tableName)
            if year==start_year:
                end_month_this_year = (12, end_month)[start_year==end_year]
                for month in range(start_month, end_month_this_year+1):
                    key_name = (str(year)+"_0"+str(month)+"_MIN_MAX"+"MINMAX",str(year)+"_"+str(month)+"_MIN_MAX"+"MINMAX")[month>=10]
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res = min(res, response['Item']['MIN_'])
            elif year==end_year:
                for month in range(1, end_month+1):
                    key_name = (str(year)+"_0"+str(month)+"_MIN_MAX"+"MINMAX",str(year)+"_"+str(month)+"_MIN_MAX"+"MINMAX")[month>=10]
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res = min(res, response['Item']['MIN_'])
            else:
                for month in range(1, 13):
                    key_name = (str(year)+"_0"+str(month)+"_MIN_MAX"+"MINMAX",str(year)+"_"+str(month)+"_MIN_MAX"+"MINMAX")[month>=10]
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res = min(res, response['Item']['MIN_'])
        return res

def get_max_temp(start_year, start_month, end_year, end_month):

    dynamodb = boto3.resource('dynamodb')

    if start_year>end_year or (start_year==end_year and start_month>end_month) or start_year<2010 or end_year>2020 or start_month<0 or start_month>12:
        print("timestamp out of range!")
        return
    else:
        res = sys.float_info.min
        for year in range(start_year, end_year+1):
            tableName = "Bikesharing"+str(year)+"_batchHelperTable"
            table = dynamodb.Table(tableName)
            if year==start_year:
                end_month_this_year = (12, end_month)[start_year==end_year]
                for month in range(start_month, end_month_this_year+1):
                    key_name = (str(year)+"_0"+str(month)+"_MIN_MAX"+"MINMAX",str(year)+"_"+str(month)+"_MIN_MAX"+"MINMAX")[month>=10]
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res = max(res, response['Item']['MAX_'])
            elif year==end_year:
                for month in range(1, end_month+1):
                    key_name = (str(year)+"_0"+str(month)+"_MIN_MAX"+"MINMAX",str(year)+"_"+str(month)+"_MIN_MAX"+"MINMAX")[month>=10]
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res = max(res, response['Item']['MAX_'])
            else:
                for month in range(1, 13):
                    key_name = (str(year)+"_0"+str(month)+"_MIN_MAX"+"MINMAX",str(year)+"_"+str(month)+"_MIN_MAX"+"MINMAX")[month>=10]
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res = max(res, response['Item']['MAX_'])
        return res

def get_num_rides(start_year, start_month, end_year, end_month):

    dynamodb = boto3.resource('dynamodb')

    if start_year>end_year or (start_year==end_year and start_month>end_month) or start_year<2010 or end_year>2020 or start_month<0 or start_month>12:
        print("timestamp out of range!")
        return
    else:
        res = 0
        for year in range(start_year, end_year+1):
            tableName = "Bikesharing"+str(year)+"_batchHelperTable"
            table = dynamodb.Table(tableName)
            if year==start_year:
                end_month_this_year = (12, end_month)[start_year==end_year]
                first_key_name = (str(year)+"_0"+str(month)+"_MIN_MAX"+"RecordsCounts",str(year)+"_"+str(month)+"_MIN_MAX"+"RecordsCounts")[start_month>=10]
                last_key_name = (str(year)+"_0"+str(month)+"_MIN_MAX"+"RecordsCounts",str(year)+"_"+str(month)+"_MIN_MAX"+"RecordsCounts")[end_month_this_year>=10]
                first_response = table.get_item(
                    Key={
                        'Key': first_key_name
                    }
                )
                last_response = table.get_item(
                    Key={
                        'Key': last_key_name
                    }
                )
                res += last_response['Item']['lastid']-first_response['Item']['firstid']
            elif year==end_year:
                first_response = table.get_item(
                    Key={
                        'Key': str(year)+"_"+"01"+"_MIN_MAX"+"RecordsCounts"
                    }
                )
                last_key_name = (str(year)+"_0"+str(month)+"_MIN_MAX"+"RecordsCounts",str(year)+"_"+str(month)+"_MIN_MAX"+"RecordsCounts")[end_month>=10]
                last_response = table.get_item(
                    Key={
                        'Key': last_key_name
                    }
                )
                res += last_response['Item']['lastid']-first_response['Item']['firstid']
            else:
                first_response = table.get_item(
                    Key={
                        'Key': str(year)+"_"+"01"+"_MIN_MAX"+"RecordsCounts"
                    }
                )
                last_response = table.get_item(
                    Key={
                        'Key': str(year)+"_"+"12"+"_MIN_MAX"+"RecordsCounts"
                    }
                )
                res += last_response['Item']['lastid']-first_response['Item']['firstid']
        return res

def get_num_membership(start_year, start_month, end_year, end_month):

    dynamodb = boto3.resource('dynamodb')

    if start_year>end_year or (start_year==end_year and start_month>end_month) or start_year<2010 or end_year>2020 or start_month<0 or start_month>12:
        print("timestamp out of range!")
        return
    else:
        res = 0
        for year in range(start_year, end_year+1):
            tableName = "Bikesharing"+str(year)+"_batchHelperTable"
            table = dynamodb.Table(tableName)
            if year==start_year:
                end_month_this_year = (12, end_month)[start_year==end_year]
                for month in range(start_month, end_month_this_year+1):
                    key_name = (str(year)+"_0"+str(month)+"_MIN_MAX"+"MINMAX",str(year)+"_"+str(month)+"_MIN_MAX"+"MembershipDistribution")[month>=10]
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res += response['Item']['MemberCounts']
            elif year==end_year:
                for month in range(1, end_month+1):
                    key_name = (str(year)+"_0"+str(month)+"_MIN_MAX"+"MINMAX",str(year)+"_"+str(month)+"_MIN_MAX"+"MembershipDistribution")[month>=10]
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res += response['Item']['MemberCounts']
            else:
                for month in range(1, 13):
                    key_name = (str(year)+"_0"+str(month)+"_MIN_MAX"+"MINMAX",str(year)+"_"+str(month)+"_MIN_MAX"+"MembershipDistribution")[month>=10]
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res += response['Item']['MemberCounts']
        return res

def get_num_casual(start_year, start_month, end_year, end_month):

    dynamodb = boto3.resource('dynamodb')

    if start_year>end_year or (start_year==end_year and start_month>end_month) or start_year<2010 or end_year>2020 or start_month<0 or start_month>12:
        print("timestamp out of range!")
        return
    else:
        res = 0
        for year in range(start_year, end_year+1):
            tableName = "Bikesharing"+str(year)+"_batchHelperTable"
            table = dynamodb.Table(tableName)
            if year==start_year:
                end_month_this_year = (12, end_month)[start_year==end_year]
                for month in range(start_month, end_month_this_year+1):
                    key_name = (str(year)+"_0"+str(month)+"_MIN_MAX"+"MINMAX",str(year)+"_"+str(month)+"_MIN_MAX"+"MembershipDistribution")[month>=10]
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res += response['Item']['CasualCounts']
            elif year==end_year:
                for month in range(1, end_month+1):
                    key_name = (str(year)+"_0"+str(month)+"_MIN_MAX"+"MINMAX",str(year)+"_"+str(month)+"_MIN_MAX"+"MembershipDistribution")[month>=10]
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res += response['Item']['CasualCounts']
            else:
                for month in range(1, 13):
                    key_name = (str(year)+"_0"+str(month)+"_MIN_MAX"+"MINMAX",str(year)+"_"+str(month)+"_MIN_MAX"+"MembershipDistribution")[month>=10]
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res += response['Item']['CasualCounts']
        return res


if __name__=="__main__":

    start_year = 2010
    end_year = 2010
    start_month = 2
    end_month = 7

    print(get_max_temp(2010,2,2010,7))
    print(get_min_temp(2010,2,2010,7))
    print(get_num_rides(2010,2,2010,7))
    print(get_num_casual(2010,2,2010,7))
    print(get_num_membership(2010,2,2010,7))
