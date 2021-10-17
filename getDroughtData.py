import requests
import zipfile
import io
import os
import subprocess
import psycopg2
from dotenv import load_dotenv
from glob import iglob



def emptyDir():
    dataPath ="./droughtData/"
    dirList = os.listdir(dataPath)
    
    for file in dirList:
        os.remove(dataPath + file)




def getDroughtData():
    print("getting drought data")

    url = 'https://droughtmonitor.unl.edu/data/shapefiles_m/USDM_current_M.zip'  
  
    response = requests.get(url)
    zippedData = zipfile.ZipFile(io.BytesIO(response.content))
    zippedData.extractall("./droughtData")




def updateDatabase():
    
    load_dotenv()

    DB_NAME = os.getenv('DB_NAME')
    PORT = os.getenv('PORT')
    USER = os.getenv('USER')
    PASSWORD = os.getenv('PASSWORD')
    HOST = os.getenv('HOST')
    DROUGHT_TABLE=os.getenv('DROUGHT_TABLE')


    conn = psycopg2.connect(dbname=DB_NAME, port=PORT, user=USER,password=PASSWORD, host=HOST)
    cur = conn.cursor()

    cur.execute("DELETE FROM {0}".format(DROUGHT_TABLE))

    conn.commit()
    conn.close()
    
    dataPath ="./droughtData/"
    dirList = [os.path.basename(file) for file in iglob(dataPath + "*.shp")]
    latestShapefile = dirList[0]

    cmd = 'shp2pgsql -a ./droughtData/{0} public.{1} | psql -q -d {2} -U {3} -w'.format(latestShapefile, DROUGHT_TABLE, DB_NAME, USER)

    subprocess.call(cmd, shell=True)



def updateDroughtData():
    emptyDir()
    getDroughtData()
    updateDatabase()