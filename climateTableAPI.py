import boto3
import sys
from datetime import date, timedelta

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

if __name__=="__main__":

    start_year = 2014
    end_year = 2018
    start_month = 3
    end_month = 7

    print('the max temp between %d-%d to %d-%d is %s'%(start_year,start_month,end_year,end_month,get_max_temp(start_year,start_month,end_year,end_month)))
    print('the min temp between %d-%d to %d-%d is %s'%(start_year,start_month,end_year,end_month,get_min_temp(start_year,start_month,end_year,end_month)))
    print('the avg temp between %d-%d to %d-%d is %s'%(start_year,start_month,end_year,end_month,get_avg_temp(start_year,start_month,end_year,end_month)))
