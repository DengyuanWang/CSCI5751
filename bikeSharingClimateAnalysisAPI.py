import boto3
import sys
from datetime import date, timedelta
import matplotlib.pyplot as plt
import numpy as np

#climate API
def get_min_temp(start_year, start_month, end_year, end_month):

    dynamodb = boto3.resource('dynamodb',region_name='us-west-2', endpoint_url="http://localhost:8000")

    if start_year>end_year or (start_year==end_year and start_month>end_month) or start_year<2010 or end_year>2020 or start_month<0 or start_month>12:
        print("timestamp out of range!")
        return
    else:
        res = sys.float_info.max
        table = dynamodb.Table("climate_2010_to_2019")
        start_date = date(start_year, start_month, 1)
        finalDay = 30;
        if end_year==2012 or end_year==2016 or end_year==2020:
            if end_month==2:
                finalDay=29
            elif (end_month<=7 and end_month%2==1) or (end_month>=8 and end_month%2==0):
                finalDay=31
        else:
            if end_month==2:
                finalDay=28
            elif (end_month<=7 and end_month%2==1) or (end_month>=8 and end_month%2==0):
                finalDay=31
        end_date = date(end_year, end_month, finalDay)
        delta = timedelta(days=1)
        while start_date <= end_date:
            key_name = str(start_date.month)+"/"+str(start_date.day)+"/"+str(start_date.year)[2:]
            response = table.get_item(
                Key={
                    'date': key_name
                }
            )
            res = min(res, float(response['Item']['Temp_Min']))
            start_date += delta
        return res

def get_max_temp(start_year, start_month, end_year, end_month):

    dynamodb = boto3.resource('dynamodb',region_name='us-west-2', endpoint_url="http://localhost:8000")

    if start_year>end_year or (start_year==end_year and start_month>end_month) or start_year<2010 or end_year>2020 or start_month<0 or start_month>12:
        print("timestamp out of range!")
        return
    else:
        res = -1
        table = dynamodb.Table("climate_2010_to_2019")
        start_date = date(start_year, start_month, 1)
        finalDay = 30;
        if end_year==2012 or end_year==2016 or end_year==2020:
            if end_month==2:
                finalDay=29
            elif (end_month<=7 and end_month%2==1) or (end_month>=8 and end_month%2==0):
                finalDay=31
        else:
            if end_month==2:
                finalDay=28
            elif (end_month<=7 and end_month%2==1) or (end_month>=8 and end_month%2==0):
                finalDay=31
        end_date = date(end_year, end_month, finalDay)
        delta = timedelta(days=1)
        while start_date <= end_date:
            key_name = str(start_date.month)+"/"+str(start_date.day)+"/"+str(start_date.year)[2:]
            response = table.get_item(
                Key={
                    'date': key_name
                }
            )
            res = max(res, float(response['Item']['Temp_Max']))
            start_date += delta
        return res

def get_avg_temp(start_year, start_month, end_year, end_month):

    dynamodb = boto3.resource('dynamodb',region_name='us-west-2', endpoint_url="http://localhost:8000")

    if start_year>end_year or (start_year==end_year and start_month>end_month) or start_year<2010 or end_year>2020 or start_month<0 or start_month>12:
        print("timestamp out of range!")
        return
    else:
        count = 0.0
        sum = 0.0
        table = dynamodb.Table("climate_2010_to_2019")
        start_date = date(start_year, start_month, 1)
        finalDay = 30;
        if end_year==2012 or end_year==2016 or end_year==2020:
            if end_month==2:
                finalDay=29
            elif (end_month<=7 and end_month%2==1) or (end_month>=8 and end_month%2==0):
                finalDay=31
        else:
            if end_month==2:
                finalDay=28
            elif (end_month<=7 and end_month%2==1) or (end_month>=8 and end_month%2==0):
                finalDay=31
        end_date = date(end_year, end_month, finalDay)
        delta = timedelta(days=1)
        while start_date <= end_date:
            key_name = str(start_date.month)+"/"+str(start_date.day)+"/"+str(start_date.year)[2:]
            response = table.get_item(
                Key={
                    'date': key_name
                }
            )
            if response['Item']['Temp_Avg']!=-1:
                sum += float(response['Item']['Temp_Avg'])
                count += 1
            start_date += delta
        if count==0:
            return -1
        else:
            return sum/count

#bike sharing API
def get_min_dur(start_year, start_month, end_year, end_month):

    dynamodb = boto3.resource('dynamodb',region_name='us-west-2', endpoint_url="http://localhost:8000")

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
                    key_name = str(year)+"_"+str(month)+"_MIN_MAX"+"MINMAX"
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res = min(res, response['Item']['MIN_'])
            elif year==end_year:
                for month in range(1, end_month+1):
                    key_name = str(year)+"_"+str(month)+"_MIN_MAX"+"MINMAX"
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res = min(res, response['Item']['MIN_'])
            else:
                for month in range(1, 13):
                    key_name = str(year)+"_"+str(month)+"_MIN_MAX"+"MINMAX"
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res = min(res, response['Item']['MIN_'])
        return res

def get_max_dur(start_year, start_month, end_year, end_month):

    dynamodb = boto3.resource('dynamodb',region_name='us-west-2', endpoint_url="http://localhost:8000")

    if start_year>end_year or (start_year==end_year and start_month>end_month) or start_year<2010 or end_year>2020 or start_month<0 or start_month>12:
        print("timestamp out of range!")
        return
    else:
        res = sys.float_info.min
        for year in range(start_year, end_year+1):
            tableName = "Bikesharing"+str(year)+"_batchHelperTable"
            table = dynamodb.Table(tableName)
            print(tableName)
            if year==start_year:
                end_month_this_year = (12, end_month)[start_year==end_year]
                for month in range(start_month, end_month_this_year+1):
                    key_name = str(year)+"_"+str(month)+"_MIN_MAX"+"MINMAX"
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res = max(res, response['Item']['MAX_'])
            elif year==end_year:
                for month in range(1, end_month+1):
                    key_name = str(year)+"_"+str(month)+"_MIN_MAX"+"MINMAX"
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res = max(res, response['Item']['MAX_'])
            else:
                for month in range(1, 13):
                    key_name = str(year)+"_"+str(month)+"_MIN_MAX"+"MINMAX"
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res = max(res, response['Item']['MAX_'])
        return res

def get_num_rides(start_year, start_month, end_year, end_month):

    dynamodb = boto3.resource('dynamodb',region_name='us-west-2', endpoint_url="http://localhost:8000")

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
                first_key_name = str(year)+"_"+str(start_month)+"_MIN_MAX"+"RecordsCounts"
                last_key_name = str(year)+"_"+str(end_month_this_year)+"_MIN_MAX"+"RecordsCounts"
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
                        'Key': str(year)+"_"+"1"+"_MIN_MAX"+"RecordsCounts"
                    }
                )
                last_key_name = str(year)+"_"+str(end_month)+"_MIN_MAX"+"RecordsCounts"
                last_response = table.get_item(
                    Key={
                        'Key': last_key_name
                    }
                )
                res += last_response['Item']['lastid']-first_response['Item']['firstid']
            else:
                first_response = table.get_item(
                    Key={
                        'Key': str(year)+"_"+"1"+"_MIN_MAX"+"RecordsCounts"
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

    dynamodb = boto3.resource('dynamodb',region_name='us-west-2', endpoint_url="http://localhost:8000")

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
                    key_name = str(year)+"_"+str(month)+"_MIN_MAX"+"MembershipDistribution"
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res += response['Item']['MemberCounts']
            elif year==end_year:
                for month in range(1, end_month+1):
                    key_name = str(year)+"_"+str(month)+"_MIN_MAX"+"MembershipDistribution"
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res += response['Item']['MemberCounts']
            else:
                for month in range(1, 13):
                    key_name = str(year)+"_"+str(month)+"_MIN_MAX"+"MembershipDistribution"
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res += response['Item']['MemberCounts']
        return res

def get_num_casual(start_year, start_month, end_year, end_month):

    dynamodb = boto3.resource('dynamodb',region_name='us-west-2', endpoint_url="http://localhost:8000")

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
                    key_name = str(year)+"_"+str(month)+"_MIN_MAX"+"MembershipDistribution"
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res += response['Item']['CasualCounts']
            elif year==end_year:
                for month in range(1, end_month+1):
                    key_name = str(year)+"_"+str(month)+"_MIN_MAX"+"MembershipDistribution"
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res += response['Item']['CasualCounts']
            else:
                for month in range(1, 13):
                    key_name = str(year)+"_"+str(month)+"_MIN_MAX"+"MembershipDistribution"
                    response = table.get_item(
                        Key={
                            'Key': key_name
                        }
                    )
                    res += response['Item']['CasualCounts']
        return res

def get_num_rides_membership_casual(year, month):
    dynamodb = boto3.resource('dynamodb',region_name='us-west-2', endpoint_url="http://localhost:8000")
    count_membership = 0
    count_casual = 0
    tableName = "Bikesharing"+str(year)+"_batchHelperTable"
    table = dynamodb.Table(tableName)
    response = table.get_item(
        Key={
            'Key': str(year)+"_"+str(month)+"_MIN_MAX"+"RecordsCounts"
        }
    )
    firstid = response['Item']['firstid']
    lastid = response['Item']['lastid']
    tableName = "Bikesharing"+str(year)
    table = dynamodb.Table(tableName)
    for index in range(firstid, lastid+1):
        response = = table.get_item(
            Key={
                'Index': index
            }
        )
        if response['Item']['Membertype'] == 'Member':
            count_membership += 1
        elif response['Item']['Membertype'] == 'Casual':
            count_casual += 1
    return count_membership, count_casual

def get_frequency_membership_casual(year, month):
    dynamodb = boto3.resource('dynamodb',region_name='us-west-2', endpoint_url="http://localhost:8000")
    time_membership = 0
    time_casual = 0
    count_membership = 0
    count_casual = 0
    tableName = "Bikesharing"+str(year)+"_batchHelperTable"
    table = dynamodb.Table(tableName)
    response = table.get_item(
        Key={
            'Key': str(year)+"_"+str(month)+"_MIN_MAX"+"RecordsCounts"
        }
    )
    firstid = response['Item']['firstid']
    lastid = response['Item']['lastid']
    tableName = "Bikesharing"+str(year)
    table = dynamodb.Table(tableName)
    for index in range(firstid, lastid+1):
        response = table.get_item(
            Key={
                'Index': index
            }
        )
        if response['Item']['Membertype'] == 'Member':
            count_membership += 1
            time_membership += response['Item']['Duration']
        elif response['Item']['Membertype'] == 'Casual':
            count_casual += 1
            time_casual += response['Item']['Duration']
    return time_membership/count_membership, time_casual/count_casual

def get_used_time(year, start_month, end_month):
    dynamodb = boto3.resource('dynamodb',region_name='us-west-2', endpoint_url="http://localhost:8000")
    bikeUsedTime = {}
    tableName = "Bikesharing"+str(year)+"_batchHelperTable"
    table = dynamodb.Table(tableName)
    first_response = table.get_item(
        Key={
            'Key': str(year)+"_"+str(start_month)+"_MIN_MAX"+"RecordsCounts"
        }
    )
    last_response = table.get_item(
        Key={
            'Key': str(year)+"_"+str(end_month)+"_MIN_MAX"+"RecordsCounts"
        }
    )
    firstid = first_response['Item']['firstid']
    lastid = last_response['Item']['lastid']
    tableName = "Bikesharing"+str(year)
    table = dynamodb.Table(tableName)
    for index in range(firstid, lastid+1):
        response = table.get_item(
            Key={
                'Index': index
            }
        )
        bikeNumber = response['Item']['Bikenumber']
        duration = response['Item']['Duration']
        if bikeNumber in bikeUsedTime:
            bikeUsedTime[bikeNumber] = bikeUsedTime.get(bikeNumber)+duration
        else:
            bikeUsedTime[bikeNumber] = duration
    return bikeUsedTime

def get_pop_density(year, start_month, end_month):
    dynamodb = boto3.resource('dynamodb',region_name='us-west-2', endpoint_url="http://localhost:8000")
    stationPopDense = {}
    tableName = "Bikesharing"+str(year)+"_batchHelperTable"
    table = dynamodb.Table(tableName)
    first_response = table.get_item(
        Key={
            'Key': str(year)+"_"+str(start_month)+"_MIN_MAX"+"RecordsCounts"
        }
    )
    last_response = table.get_item(
        Key={
            'Key': str(year)+"_"+str(end_month)+"_MIN_MAX"+"RecordsCounts"
        }
    )
    firstid = first_response['Item']['firstid']
    lastid = last_response['Item']['lastid']
    tableName = "Bikesharing"+str(year)
    table = dynamodb.Table(tableName)
    for index in range(firstid, lastid+1):
        response = table.get_item(
            Key={
                'Index': index
            }
        )
        startStation = response['Item']['Startstation']
        endStation = response['Item']['Endstation']
        if startStation in stationPopDense:
            stationPopDense[startStation] = stationPopDense.get(startStation)+1
        else:
            stationPopDense[startStation] = 1
        if endStation in stationPopDense:
            stationPopDense[endStation] = stationPopDense.get(endStation)+1
        else:
            stationPopDense[endStation] = 1
    return stationPopDense

#main function
if __name__=="__main__":
    #------------------------------basic functions---------------------------
    start_year = 2010
    start_month = 10
    end_year = 2011
    end_month = 12
    print('the max temp from %d-%d to %d-%d is %d'%(start_year, start_month, end_year, end_month, get_max_temp(start_year,start_month,end_year,end_month)))
    print('the min temp from %d-%d to %d-%d is %d'%(start_year, start_month, end_year, end_month, get_min_temp(start_year,start_month,end_year,end_month)))
    print('the min duration from %d-%d to %d-%d is %d'%(start_year, start_month, end_year, end_month, get_min_dur(start_year,start_month,end_year,end_month)))
    print('the max duration from %d-%d to %d-%d is %d'%(start_year, start_month, end_year, end_month, get_max_dur(start_year,start_month,end_year,end_month)))
    print('the numb of rides from %d-%d to %d-%d is %d'%(start_year, start_month, end_year, end_month, get_num_rides(start_year,start_month,end_year,end_month)))
    print('the numb of mem rides from %d-%d to %d-%d is %d'%(start_year, start_month, end_year, end_month, get_num_membership(start_year,start_month,end_year,end_month)))
    print('the numb of casual rides from %d-%d to %d-%d is %d'%(start_year, start_month, end_year, end_month, get_num_casual(start_year,start_month,end_year,end_month)))
    #------------------------------membership analysis---------------------------
    for year in range(2011, 2014):
        for month in range(1,13):
            print('----------%d%d----------'%(year,month))
            memRides, casualRides = get_num_rides_membership_casual(year,month)
            freMemRides, freCasualRides = get_frequency_membership_casual(year,month)
            print('num of rides: membership = %d, casual = %d'%(memRides, casualRides))
            print('frequency of rides: membership = %d, casual = %d'%(freMemRides, freCasualRides))
    #------------------------------bike used time analysis---------------------------
    print('total used time for each bike in %d-%d'%(end_year,end_month))
    print(get_used_time(end_year,end_month,end_month))
    #------------------------------population dense analysis---------------------------
    for year in range(2011, 2012):
        for month in range(1,13):
            print('----------%d%d----------'%(year,month))
            print(get_pop_density(year,month,month))
    #------------------------------correlation between climate and bike sharing---------------------------
    maxTempByMonth = np.array([])
    ridingNumByMonth = np.array([])
    for year in range(2011,2013):
        for month in range(1,13):
            maxTempByMonth = np.append(maxTempByMonth,(float)(get_max_temp(year,month,year,month)))
            ridingNumByMonth = np.append(ridingNumByMonth,(float)(get_num_rides(year,month,year,month)))
    plt.plot(maxTempByMonth, ridingNumByMonth, 'o')
    m, b = np.polyfit(maxTempByMonth, ridingNumByMonth, 1)
    plt.plot(maxTempByMonth, m*maxTempByMonth + b)
    plt.xlabel('maxTempByMonth')
    plt.ylabel('ridingNumByMonth')
    plt.title('correlation between riding numbers and max temperature by month')
    plt.show()
