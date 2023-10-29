# Import modules
from flask import Flask, jsonify
import pandas as pd
from datetime import datetime as dt
from tabulate import tabulate
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, inspect, func
from dateutil.relativedelta import relativedelta
from collections import defaultdict

# Set up DB connections

engine = create_engine("sqlite:////Users/jr/Desktop/sqlalchemy-challenge/Climate Hawaii/Resources/hawaii.sqlite")

Base = automap_base()
Base.prepare(autoload_with=engine)

meas = Base.classes.measurement
sta = Base.classes.station

app = Flask(__name__)

# Define routes

@app.route("/")
def home():
    return (
    f"Available Routes:<br/>"
    f"/api/v1.0/precipitation<br/>"
    f"/api/v1.0/stations<br/>"
    f"/api/v1.0/tobs<br/>"
)

@app.route("/api/v1.0/precipitation")
def prcp():

    session = Session(engine)

    sel = [func.max(meas.date)]
    date_max_response = session.query(*sel)
    date_max_val = date_max_response[0][0]

    # Calculate the date one year from the last date in data set.
    query_date = dt.strptime(date_max_val, '%Y-%m-%d').date()
    query_date = (query_date - relativedelta(months = 12))

    # Perform a query to retrieve the data and precipitation scores
    selections = [meas.date, meas.prcp]
    results = session.query(*selections).filter(meas.date >= query_date)
    res_list = [val for val in results]

    # Convert query results to JSON
    res_dict = defaultdict(list)

    session.close()

    for date, prcp in res_list:
        res_dict[date].append(prcp)
    return jsonify(res_dict)

@app.route("/api/v1.0/stations")
def stations():

    session = Session(engine)

    # Perform query to retrieve stations and names
    sel = [sta.station, sta.name]
    results = session.query(*sel)

    # Convert query results to JSON
    res_dict = defaultdict(list)
    for station, name in results:
        res_dict[station] = name

    session.close()

    return jsonify(res_dict)


@app.route("/api/v1.0/tobs")
def tobs():

    session = Session(engine)

    # determine most active station
    sel = [meas.station, func.count(meas.station).label('count')]
    results = session.query(*sel).order_by(sqlalchemy.desc('count')).group_by('station')
    freq_sta = results[0][0]
    freq_sta

    # query previous year of data

    # determine most recent measurement of most active station
    sel = [func.max(meas.date)]
    date_max_response = session.query(*sel).filter(meas.station == freq_sta)
    date_max_val = date_max_response[0][0]

    # Calculate the date one year from the last date in data set.
    query_date = dt.strptime(date_max_val, '%Y-%m-%d').date()
    query_date = (query_date - relativedelta(months = 12))

    sel = [meas.date, meas.tobs]
    results = session.query(*sel).filter(meas.station == freq_sta, meas.date >= query_date)

    res_dict = defaultdict(list)
    for date, tobs in results:
        res_dict[date] = tobs

    session.close()

    return jsonify(res_dict)

@app.route("/api/v1.0/<start>/")
def date_range(start, end = dt.utcnow()):

    session = Session(engine)
    # Store date filters   
    start_date = dt.strptime(start, '%Y-%m-%d').date()
    if type(end) == str:
        end_date = end
    else:
        end_date = end.date()

    # Set up parameters
    sel = [func.min(meas.tobs), func.avg(meas.tobs), func.max(meas.tobs)]
    filters = [meas.date >= start_date, meas.date <= end_date]
    # Run query
    results = session.query(*sel).filter(meas.date >= start_date, meas.date <= end_date)
    
    res_dict = defaultdict(list)

    for _min, _avg, _max in results:
        res_dict[f'{start_date} to {end_date}'] = [_min, round(_avg, 2), _max]
    
    session.close()
    return jsonify(res_dict)

@app.route("/api/v1.0/<start>/<end>")
def date_range1(start, end = dt.utcnow()):

    session = Session(engine)
    # Store date filters   
    start_date = dt.strptime(start, '%Y-%m-%d').date()
    end_date = end
    # Set up parameters
    sel = [func.min(meas.tobs), func.avg(meas.tobs), func.max(meas.tobs)]
    filters = [meas.date >= start_date, meas.date <= end_date]
    # Run query
    results = session.query(*sel).filter(meas.date >= start_date, meas.date <= end_date)
    
    res_dict = defaultdict(list)

    for _min, _avg, _max in results:
        res_dict[f'{start_date} to {end_date}'] = [_min, round(_avg, 2), _max]
    
    session.close()
    
    return jsonify(res_dict)



if __name__ == '__main__':
    app.run(debug=True)