import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

import json
from datetime import datetime, timedelta
import random
import psycopg2 as db

date_format = "%d/%m/%Y %H:%M:%S"

import sys
import subprocess

subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'faker'])
from faker import Faker


# generate new cars entry and exit

fake=Faker()

class CarPark:
    def __init__(self, Plate, LocationID, Entry_DateTime, Exit_DateTime, Parking_Charges):
        self.Plate = Plate
        self.LocationID = LocationID
        self.Entry_DateTime = Entry_DateTime
        self.Exit_DateTime = Exit_DateTime
        self.Parking_Charges = Parking_Charges

def createNewCarEntryNow():
    # days  = random.randint(1, 60) # 2-3 months
    hours = 0 #random.randint(9, 20)
    minutes = random.randint(1, 5)
    seconds = random.randint(1, 60)

    ts = datetime.now() - timedelta(hours=hours, minutes=minutes, seconds=seconds)

    hours = 0 #random.randint(0, 9) # not more than 12 hours
    minutes = random.randint(1, 5)
    seconds = random.randint(1, 60)

    duration = timedelta(hours=hours, minutes=minutes, seconds=seconds)
    exit_datetime = ts - duration
    charged = duration.seconds/60/60/2 * 60/100

    car = CarPark(
        Plate= fake.license_plate(),
        LocationID="Park"+str(random.randint(0, 5)),
        Entry_DateTime=ts.strftime(date_format), #.isoformat(),
        Exit_DateTime=exit_datetime.strftime(date_format),
        Parking_Charges=charged
    )

    return json.dumps(car.__dict__) #, sort_keys=True, indent=4)

def insertPostgresql():

    # Generate more cars, append to list and save csv
    carpark_system = []
    for i in range(10):
        thiscar_dict = eval(createNewCarEntryNow())
        carpark_system.append(list(thiscar_dict.values()))

    df = pd.DataFrame(carpark_system, columns=["Plate", "LocationID", "Entry_DateTime", "Exit_DateTime", "Parking_Charges"])
    df['Entry_DateTime'] = pd.to_datetime(df['Entry_DateTime'],format=date_format)
    df['Exit_DateTime'] = pd.to_datetime(df['Exit_DateTime'],format=date_format)
    data = [tuple(item) for index, item in df.iterrows()]
    data_for_db = tuple(data)


    # replace localhost with host.docker.internal
    conn_string="dbname='carpark_system' host='host.docker.internal' user='airflow' password='airflow'"

    conn=db.connect(conn_string)
    cur=conn.cursor()

    # insert multiple records in a single statement
    # data = []
    query = 'INSERT INTO public."CarPark"("Plate", "LocationID", "Entry_DateTime", "Exit_DateTime", "Parking_Charges") VALUES (%s, %s, %s, %s, %s)'
    # for index, item in df.iterrows():
    #     if index > 0:
    #         data.append(tuple(item))
    # data_for_db = tuple(data)
    cur.mogrify(query,data_for_db[0])
    # execute the query
    cur.executemany(query,data_for_db)

    # make it permanent by committing the transaction
    conn.commit()

    # df=pd.read_sql('select * from public."CarPark"',conn)

    # df.to_csv('/opt/airflow/dags/data/carpark_system.csv')

    print("-------Data Saved------")


default_args = {
    'owner': 'gmscher',
    'start_date': dt.datetime(2024, 7, 30),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG('carpark_system_generate_cars_DBdag',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5),      
                           # '0 * * * *',
         ) as dag:

    insertData = PythonOperator(task_id='GenerateCars',
                                python_callable=insertPostgresql)

insertData

