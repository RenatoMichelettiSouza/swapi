import requests
import json
import datetime as dt
import mysql.connector


def db_connec():
    '''
    MySql Database connection string
    '''
    return mysql.connector.connect(
        host="localhost",
        user="dev",
        password="P4ssw0rd",
        database="swapi"
    )


def page_length(count):
    
    '''
    Pagination length
    '''
    
    if (count/10) % 2 == 0:
        return count
    else:
        return int((count/10)+2)
    
def requests_get(api):
    
    '''
    API Request
    Parameter: api
    Example: https://swapi.dev/api/people/?format=json
    '''
    
    if api:
        ret = {}
        query = requests.get(api)
        query_convert = query.content.decode('utf8')
        status_code = query.status_code

        if status_code != 200:
            ret['status_code'] = status_code
            print('Error - Something weird happended.')
            exit() #**********************
        else:
            ret['status_code'] = status_code
            ret['api_return'] = json.loads(query_convert)
            print('--- COLLECTING DATA FROM: ' + api)
            print('+++ API Request Code: ' + str(status_code) + ' - Success! +++')
            print()
        return ret
    else:
        return false
