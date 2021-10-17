from bs4 import BeautifulSoup
import requests
import io
import zipfile
import os
import subprocess
import psycopg2
from dotenv import load_dotenv
from glob import glob, iglob




def emptyDir():
    dataPath ="./hurricaneData/"
    dirList = os.listdir(dataPath)
    
    for file in dirList:
        os.remove(dataPath + file)



def getHurricaneData():
    url = "https://www.nhc.noaa.gov/gis/"
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')

    links = soup.select('a[href*="5day_latest"]')
    
    for link in links:
        downloadURL = url + link['href']
        response = requests.get(downloadURL)
        zippedData = zipfile.ZipFile(io.BytesIO(response.content))
        zippedData.extractall("./hurricaneData")



def updateDatabase():
    
    load_dotenv()

    DB_NAME = os.getenv('DB_NAME')
    PORT = os.getenv('PORT')
    USER = os.getenv('USER')
    PASSWORD = os.getenv('PASSWORD')
    HOST = os.getenv('HOST')
    LINE_TABLE = os.getenv('LINE_TABLE')
    POINTS_TABLE = os.getenv('POINTS_TABLE')
    POLYGON_TABLE = os.getenv('POLYGON_TABLE')

    hurricaneTables = {
        "lin": LINE_TABLE,
        "pgn": POLYGON_TABLE,
        "pts": POINTS_TABLE
    }


    conn = psycopg2.connect(dbname=DB_NAME, port=PORT, user=USER,password=PASSWORD, host=HOST)
    cur = conn.cursor()

    for table in [LINE_TABLE, POINTS_TABLE, POLYGON_TABLE]:
        cur.execute("DELETE FROM {0}".format(table))

    conn.commit()
    conn.close()
    
    
    dataPath ="./hurricaneData/"
    dirList = [os.path.basename(file) for file in iglob(dataPath + "*.shp")]

    for shapefile in dirList:
        if '5day_' in shapefile:
            shapefileType = shapefile.split('.shp')[0].split('5day_')[1]
            table = hurricaneTables[shapefileType]

            shapefilePath = dataPath + shapefile
            cmd = 'shp2pgsql -a {0} public.{1} | psql -q -d {2} -U {3} -w'.format(shapefilePath, table, DB_NAME, USER)
            subprocess.call(cmd, shell=True)


def updateHurricaneData():
    emptyDir()
    getHurricaneData()
    updateDatabase()