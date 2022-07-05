import csv
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, Float
import sqlite3

engine = create_engine('sqlite:///database.db')

#Connection with database
conn = sqlite3.connect('database.db')
cur = conn.cursor()

meta = MetaData()

#Creating measure table
measure = Table(
    'measure', meta,
    Column('station', String),
    Column('date', String),
    Column('precip', Float),
    Column('tobs', Integer)
)

#Creating stations table
stations = Table(
    'stations', meta,
    Column('station', String),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('elevation', Float),
    Column('name', String),
    Column('country', String),
    Column('state', String)
)

meta.create_all(engine)
print(engine.table_names())


#Adding data from clean_measure.csv to database.db 
with open('clean_measure.csv') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    line = 0
    for row in csv_reader:
        station = row["station"]
        date = row["date"]
        precip = row["precip"]
        tobs = row["tobs"]
        cur.execute('''INSERT INTO measure(station,date,precip,tobs) VALUES (?,?,?,?)''', (station,date,precip,tobs))
        conn.commit()
      
        
#Adding data from clean_stations.csv to database.db 
with open('clean_stations.csv') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        station = row["station"]
        latitude = row["latitude"]
        longitude = row["longitude"]
        elevation = row["elevation"]
        name = row["name"]
        country = row["country"] 
        state = row["state"]
        
        cur.execute('''INSERT INTO stations(station,latitude,longitude,elevation,name,country,state) VALUES (?,?,?,?,?,?,?)''', (station,latitude,longitude,elevation,name,country,state))
        conn.commit()

cur.execute("SELECT * FROM stations LIMIT 5")
row = cur.fetchall()
print(row)