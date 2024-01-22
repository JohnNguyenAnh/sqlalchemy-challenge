# Import the dependencies.
from flask import Flask, jsonify
from sqlalchemy import create_engine, func, desc,  distinct
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
Base = automap_base()
Base.prepare(engine, reflect=True)

# References to the tables
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Routes
#################################################

@app.route("/")
def home():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start&gt;<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;<br/>"
    )
    
@app.route("/api/v1.0/precipitation")
def precipitation():
    session = Session(engine)
    """Return the precipitation data for the last year"""
    # Find the most recent date in the data set.
    most_recent_date = session.query(Measurement.date)\
    .order_by(desc(Measurement.date))\
    .first()
    # Calculate the date one year from the last date in data set.
    one_year_ago = datetime.strptime(most_recent_date[0], '%Y-%m-%d')   - timedelta(days=366)
    # Query to retrieve the data
    results = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= one_year_ago).all()
    session.close()

    # Convert to dictionary
    precipitation_dict = {date: prcp for date, prcp in results}
    return jsonify(precipitation_dict)

@app.route("/api/v1.0/stations")
def stations():
    session = Session(engine)
    """Return a list of stations"""
    results = session.query(Station.station).all()
    session.close()

    # Convert list of tuples into a normal list
    stations_list = list(np.ravel(results))
    return jsonify(stations_list)

@app.route("/api/v1.0/tobs")
def tobs():
    session = Session(engine)
    """Return the temperature observations for the last year of the most active station"""
    # Find the most recent date in the data set.
    most_recent_date = session.query(Measurement.date)\
    .order_by(desc(Measurement.date))\
    .first()
    # Calculate the date one year from the last date in data set.
    one_year_ago = datetime.strptime(most_recent_date[0], '%Y-%m-%d') - timedelta(days=366)
    # Query to retrieve the data
    results = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= one_year_ago).all()
    #Query all stations name and count how many time it appear
    most_active_stations = session.query(
        Measurement.station, 
        func.count(Measurement.station)
    ).group_by(
        Measurement.station
    ).order_by(
        func.count(Measurement.station).desc()
    ).first()
    #Query only the station name and count of the most active
    most_active_station_id = most_active_stations[0]
    # Query for the most active station and for the last year data
    results = session.query(Measurement.tobs).\
        filter(Measurement.station == most_active_station_id).\
        filter(Measurement.date >= one_year_ago).all()
    session.close()

    tobs_list = list(np.ravel(results))
    return jsonify(tobs_list)


@app.route("/api/v1.0/<start>")
def start(start):
    session = Session(engine)
    try:
        start_date = datetime.strptime(start, '%Y-%m-%d')
    except ValueError:
        session.close()
        return jsonify({"error": "Date format should be YYYY-MM-DD"}), 400
    """Return TMIN, TAVG, TMAX for dates start from url"""
    results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).all()
    session.close()

    temp_stats = list(np.ravel(results))
    return jsonify(temp_stats)


@app.route("/api/v1.0/<start>/<end>")
def start_end(start, end):
    session = Session(engine)
    try:
        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d')
    except ValueError:
        session.close()
        return jsonify({"error": "Date format should be YYYY-MM-DD"}), 400
    """Return TMIN, TAVG, TMAX for dates between the start and end date inclusive"""
    results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).\
        filter(Measurement.date <= end_date).all()
    session.close()

    temp_stats = list(np.ravel(results))
    return jsonify(temp_stats)

if __name__ == '__main__':
    app.run(debug=True)









