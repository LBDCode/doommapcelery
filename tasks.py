from celery import Celery
from dotenv import load_dotenv
from getAdvisoryData import updateAdvisoryData
from getActiveFires import updateFireData
from getDroughtData import updateDroughtData
from getHurricaneData import updateHurricaneData
from celery.schedules import crontab

app = Celery('tasks', broker='pyamqp://guest@localhost//')



@app.task
def updateAdvisoryDataTask():
    updateAdvisoryData()

@app.task
def updateFireDataTask():
    updateFireData()

@app.task
def updateDroughtDataTask():
    updateDroughtData()

@app.task
def updateHurricaneDataTask():
    updateHurricaneData()