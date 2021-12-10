# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

from fastapi import FastAPI
from typing import List, Optional
from fastapi import FastAPI, Query
import mysql.connector

config = {
  'user': 'root',
  'password': 'test123',
  'host': 'db',
  'database': 'test',
  'raise_on_warnings': True
}

app = FastAPI()

@app.get("/confirmed")
def read_root():
    test = ""
   # try:
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor(buffered=True, dictionary=True)

    query = ("select * from cases_confirmedCV19")
    cursor.execute(query, params=None)
    data = []
    for row in cursor:
        data.append(row)
    #except mysql.connector.Error as err:
        #print("Something went wrong: {}".format(err))

    cursor.close()
    cnx.close()

    return {"data": data}

@app.get("/death")
def read_root():
    test = ""
   # try:
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor(buffered=True, dictionary=True)

    query = ("select * from cases_deathCV19")
    cursor.execute(query, params=None)
    data = []
    for row in cursor:
        data.append(row)
    #except mysql.connector.Error as err:
        #print("Something went wrong: {}".format(err))

    cursor.close()
    cnx.close()

    return {"data": data}

@app.get("/recovered")
def read_root():
    test = ""
   # try:
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor(buffered=True, dictionary=True)

    query = ("select * from cases_recoveredCV19")
    cursor.execute(query, params=None)
    data = []
    for row in cursor:
        data.append(row)
    #except mysql.connector.Error as err:
        #print("Something went wrong: {}".format(err))

    cursor.close()
    cnx.close()

    return {"data": data}


@app.get("/consolidated")
def read_root():
    test = ""
   # try:
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor(buffered=True, dictionary=True)

    query = ("select * from cases_CV19")
    cursor.execute(query, params=None)
    data = []
    for row in cursor:
        data.append(row)
    #except mysql.connector.Error as err:
        #print("Something went wrong: {}".format(err))

    cursor.close()
    cnx.close()

    return {"data": data}

@app.get("/operaciones/suma")
async def read_items(q: Optional[List[int]] = Query(None)):
    x = 0
    for i in range(len(q)):
        x+=q[i]
    res = {"suma": x}
    return res

