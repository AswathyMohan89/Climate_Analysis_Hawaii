import datetime as dt
import numpy as np
import pandas as pd
import os
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify
#################################################
# Database Setup
#################################################
engine = create_engine(os.path.join("sqlite:///","..","Resources","hawaii.db"),echo=False)

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
measurements = Base.classes.Measurements
stations= Base.classes.Stations

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end>"

    )


@app.route("/api/v1.0/precipitation")
def prcp():

	result = session.execute("SELECT max(date) FROM measurements order by date ").first()
	new_d=result[0]
	month=new_d.split("-")[2]
	date=new_d.split("-")[1]
	year=new_d.split("-")[0]
	y=int(year)-1
	new_date=str(y)+"-"+date+"-"+month
	precip_qry = session.query(stations.name,measurements.date, measurements.prcp).filter(measurements.date>=new_date).order_by(measurements.date).all()
	#results = session.query(Passenger.name).all()
	precip_list=[]
	for d in precip_qry:
		precip_list.append({"Station": d[0], "Date": d[1], "Precipitation": d[2]})

	return jsonify(precip_list)


@app.route("/api/v1.0/stations")
def stations():

	results = session.execute("SELECT  distinct station,name FROM stations ").fetchall()
	all_stations = list(np.ravel(results))
	return jsonify(all_stations)

@app.route("/api/v1.0/tobs")
def tobs():
	result_date = session.execute("SELECT max(date) FROM measurements order by date ").first()
	new_d=result_date[0]
	month=new_d.split("-")[2]
	date=new_d.split("-")[1]
	year=new_d.split("-")[0]
	y=int(year)-1
	new_date=str(y)+"-"+date+"-"+month
	temp_qry = session.query(measurements.station,measurements.date, measurements.tobs).filter(measurements.date>=new_date).order_by(measurements.date).all()
	#results = session.query(Passenger.name).all()
	temp_list=[]
	for t in temp_qry:
		temp_list.append({"Station": t[0], "Date": t[1], "Temprature": t[2]})

	return jsonify(temp_list)



@app.route("/api/v1.0/<start_date>")
def start_temp(start_date):
	temp_stat=[]
	newtemp_qry=session.query(measurements.date, measurements.tobs).filter(measurements.date>=start_date).order_by(measurements.date)
	temp_df = pd.read_sql(newtemp_qry.statement, newtemp_qry.session.bind)

	max_temp=temp_df["tobs"].max()
	min_temp=temp_df["tobs"].min()
	avg_temp=temp_df["tobs"].mean()


	temp_stat.append({"Maximum":str(max_temp),"Minimum":str(min_temp),"Average":str(avg_temp)})
	
	#return str(max_temp)
	return jsonify(temp_stat)

@app.route("/api/v1.0/<start_date>/<end_date>")
def start_end_temp(start_date,end_date):
	temp_stat=[]
	newtemp_qry=session.query(measurements.date, measurements.tobs).filter(measurements.date.between(start_date,end_date)).order_by(measurements.date)
	temp_df = pd.read_sql(newtemp_qry.statement, newtemp_qry.session.bind)

	max_temp=temp_df["tobs"].max()
	min_temp=temp_df["tobs"].min()
	avg_temp=temp_df["tobs"].mean()


	temp_stat.append({"Maximum":str(max_temp),"Minimum":str(min_temp),"Average":str(avg_temp)})
	
	#return str(max_temp)
	return jsonify(temp_stat)

if __name__ == '__main__':
    app.run(debug=True,port=3316)