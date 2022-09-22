# swapi
Star Wars API Test

Pre-requisites
-----------------------------------------
Python 3.9.7
MySql


Package Content
-----------------------------------------
Folder "ddl" contains the Data Definition Language to create all database tables.

- Table 'people_raw' is the table to store all the raw data.

- Table 'people_report' is the table that will contain the data extracted from 'people_raw' and it will be the source for the report view.

- View 'oldes_charc_by_film' is the report that shows the aggregation of who was the oldest character by film.

Folder "src" contains the Python pipeline program that GET the data from the API https://swapi.dev/api/people


Steps for Execution
-----------------------------------------
1st) Apply the sql script -> ddl/create_swapi_table.sql on MySql.

2nd) run the ingestion_raw.py script that is located on 'src'
     $ python3 ingestion_raw.py

The script will GET the data from the API and store it on the raw table, once finished, the report table will be fed.

Check the result on MySql


------------------------------------------
History of the data

The history of the data can be tracked by the column 'ingested_datetime' that contains the timestamp of when the data was ingested.

