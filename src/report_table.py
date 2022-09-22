import requests
import json
import datetime as dt
import logging

import utils.swapi_functions as swapi



def insert_report_table(origin_table='swapi.people_raw', destination_table='swapi.people_report'):

    '''
    Feeding Report table `people_report`
    '''
    
    start_dt = dt.datetime.now()
    print('Feeding Report table -----> ' + destination_table)
    print('Start datetime: ' + str(start_dt))
    print(100*'-')
    print()
    mydb = swapi.db_connection()
    print()
    mycursor = mydb.cursor()
    
    sql = f'select name, birth_year, films, max(ingested_datetime) as raw_ingested_datetime from {origin_table} group by name, birth_year, films'
    
    mycursor.execute(sql)
    result = mycursor.fetchall()
    print(result)
    
    inserted_rows = 0
    
    for i in range(0, len(result)):
        ingested_datetime = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        db_values = result[i] + (f'{ingested_datetime}',)
        name = result[i][0]
        birth_year = result[i][1]
        films = result[i][2].split(',')
        raw_ingested_datetime = result[i][3]
        column_names = '(`name`, `birth_year`, `films`, `ingested_datetime`, `raw_ingested_datetime`)'
        
        film_list = []
        film_dict = {}

        for film in films:
            film_dict['name'] = name
            
            if birth_year != 'unknown':
                film_dict['birth_year'] = float(birth_year.replace('BBY',''))
            else:
                film_dict['birth_year'] = 0
            
            film_dict['film'] = film
            film_dict['ingested_datetime'] = ingested_datetime
            film_dict['raw_ingested_datetime'] = raw_ingested_datetime
            
            film_list.append(film_dict.copy())

        for i in film_list:
            db_values = str(tuple(i.values()))
            sql = f"INSERT INTO {destination_table} {column_names} VALUES {db_values};"
            inserted_rows += swapi.db_write(sql)
    print()
    print(inserted_rows, "rows inserted.")
    print(100*'-')
    print()
    
    mydb.commit()
    end_dt = dt.datetime.now()
    diff_dt = end_dt - start_dt
    print('PROCESS EXECUTED IN: ' + str(diff_dt.total_seconds()) + ' secs.')
