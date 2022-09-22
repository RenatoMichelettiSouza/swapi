#!/usr/bin/env python
# coding: utf-8

import requests
import json
import datetime as dt
import logging
import mysql.connector

import report_table as rt

import utils.swapi_functions as swapi



def df_raw(api_return, start_dt):

    '''
    Raw Data preparation to ingest
    '''
    
    people_list = []
    people_dict = {}

    for people in api_return:
        people_dict['name'] = people['name']
        people_dict['height'] = people['height']
        people_dict['mass'] = people['mass']
        people_dict['hair_color'] = people['hair_color']
        people_dict['skin_color'] = people['skin_color']
        people_dict['eye_color'] = people['eye_color']
        people_dict['birth_year'] = people['birth_year']
        people_dict['gender'] = people['gender']
        people_dict['homeworld'] = people['homeworld']
        people_dict['films'] = str(tuple(people['films'])).replace("(","").replace(")","").replace("'","").replace(" ","")
        people_dict['species'] = str(tuple(people['species'])).replace("(","").replace(")","").replace("'","").replace(" ","")
        people_dict['vehicles'] = str(tuple(people['vehicles'])).replace("(","").replace(")","").replace("'","").replace(" ","")
        people_dict['starships'] = str(tuple(people['starships'])).replace("(","").replace(")","").replace("'","").replace(" ","")
        people_dict['created'] = people['created']
        people_dict['edited'] = people['edited']
        people_dict['url'] = people['url']
        people_dict['ingested_datetime'] = start_dt.strftime("%Y-%m-%d %H:%M:%S")

        people_list.append(people_dict.copy())
        
    return people_list



def raw_ingestion(api_url, destination_table):

    '''
    Raw ingestion
    Parameters: api_url, destination_table
    Example: api_url -> https://swapi.dev/api/people/?format=json
             destination_table -> swapi.people_raw
    '''
    mydb = mysql.connector.connect(
        host="localhost",
        user="dev",
        password="P4ssw0rd!",
        database="swapi"
    )

    start_dt = dt.datetime.now()
    print('Ingesting data from API to Raw table -----> ' + destination_table)
    print('Start datetime: ' + str(start_dt))
    print(100*'-')
    print()

    # API REQUEST
    query_json = swapi.requests_get(api_url)
    api_return = query_json['api_return']
  
    columns = tuple(api_return['results'][0].keys()) + ('ingested_datetime',)
    column_names = str(columns).replace("'","`")
    
    people_list = df_raw(api_return['results'], start_dt)
    
    next_page = api_return['next']
    
    inserted_rows = 0

    # PAGINATION LENGTH
    count = swapi.page_length(api_return['count'])
    
    # INSERT DB FIRST PAGE
    for n in range(0, len(people_list)):
        print(people_list[n]['name'])
        db_values = str(tuple(people_list[n].values()))
        sql = f"INSERT INTO {destination_table} {column_names} VALUES {db_values};"
        inserted_rows += swapi.db_write(sql)
    print()
    print(inserted_rows, "rows inserted.")
    print(100*'-')
    print()
    
    # PAGINATION LOOP
    for n in range(1, count):

        query_json = swapi.requests_get(next_page)

        if query_json:
            api_return = query_json['api_return']
            next_page = api_return['next']

            people_list = df_raw(api_return['results'], start_dt)

            for n in range(0, len(people_list)):
                print(people_list[n]['name'])
                db_values = str(tuple(people_list[n].values()))
                sql = f"INSERT INTO {destination_table} {column_names} VALUES {db_values};"
                inserted_rows += swapi.db_write(sql)
            print()
            print(inserted_rows, "rows inserted.")
            print(100*'-')
            print()

        if api_return['next'] is None:
            break
            
    mydb.commit()
    mydb.close()
    
    end_dt = dt.datetime.now()
    diff_dt = end_dt - start_dt
    print('PROCESS EXECUTED IN: ' + str(diff_dt.total_seconds()) + ' secs.')
    print()
    print(100*'-')
    print()

    return True



if __name__ == "__main__":

    try:
        
        if raw_ingestion('https://swapi.dev/api/people/?format=json','swapi.people_raw'):
            rt.insert_report_table()


   
    except Exception as e:
        print('---Error---:')
        if hasattr(e, 'message'):
            logging.warning('python2')
            logging.error(str(e.message))
        else:
            logging.warning('python3')
            logging.error(str(e))
        


