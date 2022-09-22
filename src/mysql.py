import requests
import json
import datetime as dt

import mysql.connector

mydb = mysql.connector.connect(
host="localhost",
user="dev",
password="P4ssw0rd!",
database="swapi"
)

mycursor = mydb.cursor()

sql = f'select * from swapi.people_raw;'

mycursor.execute(sql)
result = mycursor.fetchall()
print(result)
