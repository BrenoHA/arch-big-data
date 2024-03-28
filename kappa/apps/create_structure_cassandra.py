from cassandra.cluster import Cluster
clstr=Cluster(['172.19.0.5', '172.19.0.4'])
session=clstr.connect()

qry=''' 
CREATE KEYSPACE IF NOT EXISTS weather_keyspace
WITH replication = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 1
};'''
	
session.execute(qry) 

qry=''' 
CREATE TABLE IF NOT EXISTS weather_keyspace.weather_table (
  date text,
  city text,
  lat float,
  lon float,
  temperature float,
  feels_like float,
  temperature_minus_feels_like float,
  PRIMARY KEY ((city, date))
);'''

session.execute(qry) 
