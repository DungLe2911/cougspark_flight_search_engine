import json
import datetime
import gmplot

from flask import Flask, render_template
from flask import request, Response, send_from_directory
from pyspark.sql import SparkSession

from ParGraphAgg import *
from SeqGraphAgg import *

app = Flask(__name__)

sga = SeqGraphAgg('cleanedv2')
pga = ParGraphAgg('cleanedv2')


@app.route("/", methods=['GET'])
def hello():
    return render_template('index.html')


@app.route("/findairportincountryseq", methods=['GET'])
def FindAirportInCountrySeq():
    start = datetime.datetime.now()
    X = request.args.get('x', '')
    res = sga.FindAirportInCountry(X)
    end = datetime.datetime.now()
    delta = end-start
    elapsed = delta.total_seconds() * 1000
    return Response(json.dumps({'elapsed(ms)': elapsed, "size": len(res), 'seq_result': res}), mimetype='application/json')


@app.route("/findairportincountrypar", methods=['GET'])
def FindAirportInCountryPar():
    start = datetime.datetime.now()
    X = request.args.get('x', '')
    res = pga.FindAirportInCountry(X)
    end = datetime.datetime.now()
    delta = end-start
    elapsed = delta.total_seconds() * 1000
    return Response(json.dumps({'elapsed(ms)': elapsed, "size": len(res), 'par_result': res}), mimetype='application/json')


@app.route("/findairlinehavingxstopseq", methods=['GET'])
def FindAirlineHavingXStopSeq():
    start = datetime.datetime.now()
    X = request.args.get('x', '')
    res = sga.FindAirlineHavingXStop(X)
    end = datetime.datetime.now()
    delta = end-start
    elapsed = delta.total_seconds() * 1000
    return Response(json.dumps({'elapsed(ms)': elapsed, "size": len(res), 'seq_result': res}), mimetype='application/json')


@app.route("/findairlinehavingxstoppar", methods=['GET'])
def FindAirlineHavingXStopPar():
    start = datetime.datetime.now()
    X = request.args.get('x', '')
    res = pga.FindAirlineHavingXStop(X)
    end = datetime.datetime.now()
    delta = end-start
    elapsed = delta.total_seconds() * 1000
    return Response(json.dumps({'elapsed(ms)': elapsed, "size": len(res), 'par_result': res}), mimetype='application/json')


@app.route("/findairlinewithcodeshareseq", methods=['GET'])
def FindAirlineWithCodeShareSeq():
    start = datetime.datetime.now()
    res = sga.FindAirlineWithCodeShare()
    end = datetime.datetime.now()
    delta = end-start
    elapsed = delta.total_seconds() * 1000
    return Response(json.dumps({'elapsed(ms)': elapsed, "size": len(res), 'seq_result': res}), mimetype='application/json')


@app.route("/findairlinewithcodesharepar", methods=['GET'])
def FindAirlineWithCodeSharePar():
    start = datetime.datetime.now()
    res = pga.FindAirlineWithCodeShare()
    end = datetime.datetime.now()
    delta = end-start
    elapsed = delta.total_seconds() * 1000
    return Response(json.dumps({'elapsed(ms)': elapsed, "size": len(res), 'par_result': res}), mimetype='application/json')


@app.route("/findcountryhashighestairprotseq", methods=['GET'])
def FindCountryHasHighestAirportSeq():
    start = datetime.datetime.now()
    res = sga.FindCountryHasHighestAirport()
    end = datetime.datetime.now()
    delta = end-start
    elapsed = delta.total_seconds() * 1000
    print(res)
    return Response(json.dumps({'elapsed(ms)': elapsed, "size": len(res), 'seq_result': res}), mimetype='application/json')


@app.route("/findcountryhashighestairprotpar", methods=['GET'])
def FindCountryHasHighestAirportPar():
    start = datetime.datetime.now()
    res = pga.FindCountryHasHighestAirport()
    end = datetime.datetime.now()
    delta = end-start
    elapsed = delta.total_seconds() * 1000
    return Response(json.dumps({'elapsed(ms)': elapsed, "size": len(res), 'par_result': res}), mimetype='application/json')


@app.route("/findtopkbusycityseq", methods=['GET'])
def FindTopKBusyCitySeq():
    start = datetime.datetime.now()
    K = request.args.get('k', '')
    res = sga.FindTopKBusyCity(K)
    res_in = res[0]
    res_out = res[1]
    end = datetime.datetime.now()
    delta = end-start
    elapsed = delta.total_seconds() * 1000
    print(res)
    return Response(json.dumps({'elapsed(ms)': elapsed, "size": len(res), 'seq_result': {"incoming": res_in, "outgoing": res_out}}), mimetype='application/json')


@app.route("/findtopkbusycitypar", methods=['GET'])
def FindTopKBusyCityPar():
    start = datetime.datetime.now()
    K = request.args.get('k', '')
    res = pga.FindTopKBusyCity(K)
    res_in = res[0]
    res_out = res[1]
    end = datetime.datetime.now()
    delta = end-start
    elapsed = delta.total_seconds() * 1000
    return Response(json.dumps({'elapsed(ms)': elapsed, "size": len(res), 'par_result': {"incoming": res_in, "outgoing": res_out}}), mimetype='application/json')


@app.route("/findtripxcitytoycityseq", methods=['GET'])
def FindTripXCityToYCitySeq():
    start = datetime.datetime.now()
    X = request.args.get('x', '')
    Y = request.args.get('y', '')
    res = sga.FindTripXCityToYCity(X, Y)
    end = datetime.datetime.now()
    delta = end-start
    elapsed = delta.total_seconds() * 1000
    return Response(json.dumps({'elapsed(ms)': int(elapsed), 'result': res}), mimetype='application/json')


@app.route("/findtripxcitytoycitypar", methods=['GET'])
def FindTripXCityToYCityPar():
    start = datetime.datetime.now()
    X = request.args.get('x', '')
    Y = request.args.get('y', '')
    res = pga.FindTripXCityToYCity(X, Y)
    end = datetime.datetime.now()
    delta = end-start
    elapsed = delta.total_seconds() * 1000
    return Response(json.dumps({'elapsed(ms)': int(elapsed), 'result': res}), mimetype='application/json')


@app.route("/findtripxtoylessthanzseq", methods=['GET'])
def FindTripXToYLessThanZSeq():
    start = datetime.datetime.now()
    X = request.args.get('x', '')
    Y = request.args.get('y', '')
    Z = request.args.get('z', '')
    res = sga.FindTripXToYLessThanZ(X, Y, Z)
    end = datetime.datetime.now()
    delta = end-start
    elapsed = delta.total_seconds() * 1000
    return Response(json.dumps({'elapsed(ms)': int(elapsed), 'result': res}), mimetype='application/json')


@app.route("/findtripxtoylessthanzpar", methods=['GET'])
def FindTripXToYLessThanZPar():
    start = datetime.datetime.now()
    X = request.args.get('x', '')
    Y = request.args.get('y', '')
    Z = request.args.get('z', '')
    assert(int(Z) < 4 or int(Z) > 0)
    result = []
    for i in range(1, int(Z)+1):
        res = pga.FindTripXToYLessThanZ(X, Y, i)
        if(len(res) > 0):
            result.append(res)
            break
    end = datetime.datetime.now()
    delta = end-start
    elapsed = delta.total_seconds() * 1000
    return Response(json.dumps({'elapsed(ms)': int(elapsed), 'result': result}), mimetype='application/json')


@app.route("/finddhopcitiesseq", methods=['GET'])
def FindDHopCitiesSeq():
    start = datetime.datetime.now()
    D = request.args.get('d', '')
    X = request.args.get('x', '')
    res = sga.FindDHopCities(D, X)
    end = datetime.datetime.now()
    delta = end-start
    elapsed = delta.total_seconds() * 1000
    return Response(json.dumps({'elapsed(ms)': int(elapsed), "size": len(res), 'result': res}), mimetype='application/json')


@app.route("/finddhopcitiespar", methods=['GET'])
def FindDHopCitiesPar():
    start = datetime.datetime.now()
    D = request.args.get('d', '')
    X = request.args.get('x', '')
    res = pga.FindDHopCities(D, X)
    end = datetime.datetime.now()
    delta = end-start
    elapsed = delta.total_seconds() * 1000
    return Response(json.dumps({'elapsed(ms)': int(elapsed), "size": len(res), 'result': res}), mimetype='application/json')


@app.route("/drawgraph", methods=['GET'])
def DrawGraph():
    # declare the center of the map, and how much we want the map zoomed in
    geo_lat = [47.449001, 37.469101, 35.987955]
    geo_lon = [-122.308998, 126.450996, 129.420383]
    geo_lat2 = [47.449001, 35.987955]
    geo_lon2 = [-122.308998, 129.420383]
    gmap = gmplot.GoogleMapPlotter(0, 180, 3)
    # plot heatmap
    gmap.plot(geo_lat, geo_lon, edge_width=3.0)
    gmap.plot(geo_lat2, geo_lon2, edge_width=3.0)
    # Your Google_API_Key
    gmap.apikey = "AIzaSyBrJkwsZLNnwLCwM5Ae-38M_Ua9ngn4Xts"
    # save it to html
    gmap.draw(r"./result.html")
    return send_from_directory(".", "result.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0")
