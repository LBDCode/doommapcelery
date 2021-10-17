
import requests
import io
import psycopg2
from psycopg2.sql import Identifier, SQL
from decimal import Decimal
import os
from dotenv import load_dotenv
import datetime



load_dotenv()


def updateFireData():

    DB_NAME = os.getenv('DB_NAME')
    PORT = os.getenv('PORT')
    USER = os.getenv('USER')
    PASSWORD = os.getenv('PASSWORD')
    HOST = os.getenv('HOST')
    FIRE_TABLE = os.getenv('FIRE_TABLE')

    conn = psycopg2.connect(dbname=DB_NAME, port=PORT, user=USER,password=PASSWORD, host=HOST)

    cur = conn.cursor()


    cur.execute("DELETE FROM {0}".format(FIRE_TABLE))
    conn.commit()

    url = "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/Current_WildlandFire_Locations/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json" 
    response = requests.get(url).json()
    fires = response['features']


    for fire in fires:
        
        if fire["attributes"]["DailyAcres"] and fire["attributes"]["DailyAcres"] > 100:
            OBJECTID = fire["attributes"]["OBJECTID"]
            DailyAcres = fire["attributes"]["DailyAcres"]
            EstimatedCostToDate = fire["attributes"]["EstimatedCostToDate"]
            FireBehaviorGeneral = fire["attributes"]["FireBehaviorGeneral"]
            FireBehaviorGeneral1 = fire["attributes"]["FireBehaviorGeneral1"]
            FireBehaviorGeneral2 = fire["attributes"]["FireBehaviorGeneral2"]
            FireBehaviorGeneral3 = fire["attributes"]["FireBehaviorGeneral3"]
            FireDiscoveryDateTime = datetime.datetime.fromtimestamp((fire["attributes"]["FireDiscoveryDateTime"])/1000).strftime('%Y-%m-%d %H:%M:%S')
            FireMgmtComplexity = fire["attributes"]["FireMgmtComplexity"]
            IncidentName = fire["attributes"]["IncidentName"]
            IncidentShortDescription = fire["attributes"]["IncidentShortDescription"]
            TotalIncidentPersonnel = fire["attributes"]["TotalIncidentPersonnel"]
            CreatedOnDateTime_dt = datetime.datetime.fromtimestamp((fire["attributes"]["CreatedOnDateTime_dt"])/1000).strftime('%Y-%m-%d %H:%M:%S')
            ModifiedOnDateTime_dt = datetime.datetime.fromtimestamp(fire["attributes"]["ModifiedOnDateTime_dt"]/1000).strftime('%Y-%m-%d %H:%M:%S')
            x = Decimal(fire["geometry"]["x"])
            y = Decimal(fire["geometry"]["y"])


            args_tuple = (OBJECTID, DailyAcres, EstimatedCostToDate, FireBehaviorGeneral, FireBehaviorGeneral1, FireBehaviorGeneral2, FireBehaviorGeneral3, 
                FireDiscoveryDateTime, FireMgmtComplexity, IncidentName, IncidentShortDescription, TotalIncidentPersonnel, CreatedOnDateTime_dt, ModifiedOnDateTime_dt, x, y)
            

            cur.execute(SQL("INSERT INTO {} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s),4326))")
                .format(Identifier(FIRE_TABLE)), args_tuple)


    conn.commit()
    conn.close()


