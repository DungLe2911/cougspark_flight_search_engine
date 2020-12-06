import json, datetime

from flask import Flask, request, Response
from pyspark.sql import SparkSession

from ParGraph import *

app = Flask(__name__)

pg = ParGraph('enriched')

@app.route("/", methods=['GET'])
def hello():
    return "Welcome to Space Flight Search Engine!"

@app.route("/findairlinewithcodeshare", methods=['GET'])
def FindAirlineWithCodeShare():
    res = pg.FindAirlineWithCodeShare()
    return Response(json.dumps({'result':res}), mimetype='application/json')

@app.route("/findtripxtoylessthanz", methods=['GET'])
def FindTripXToYLessThanZ():
    start = datetime.datetime.now()
    X = request.args.get('x','')
    Y = request.args.get('y','')
    Z = request.args.get('z','')
    trips = pg.FindTripXToYLessThanZ(X, Y, int(Z))
    res = []
    for trip in trips:
        trip_list = {}
        for apid in trip:
            trip_list[apid]=pg.GetAirportNameFromId(apid)
        res.append(trip_list)
    end = datetime.datetime.now()
    delta = end-start
    elapsed = delta.total_seconds() * 1000
    return Response(json.dumps({'elapsed(ms)':int(elapsed),'result':res}), mimetype='application/json')
    

if __name__ == "__main__":
    app.run(host="0.0.0.0")