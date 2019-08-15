from flask import Flask, jsonify
from flask_cors import CORS
import requests
# import dateutil.parser
# import requests_toolbelt.adapters.appengine

# # Use the App Engine Requests adapter. This makes sure that Requests uses
# # URLFetch.
# requests_toolbelt.adapters.appengine.monkeypatch()


app = Flask(__name__)
CORS(app)

@app.route("/sensors")
def index():
  url = 'https://api.sealevelsensors.org/v1.0/Things?$expand=Datastreams($expand=ObservedProperty,Observations,Sensor),Locations'
  r = requests.get(url)
  sensors = r.json()['value']

  ret = []
  for sensor in sensors:
    _id = sensor['@iot.id']
    name = sensor['name']
    description = sensor['description']
    lng, lat = sensor['Locations'][0]['location']['coordinates']

    elevation_navd88 = 0
    if 'elevationNAVD88' in sensor['properties']:
      elevation_navd88 = float(sensor['properties']['elevationNAVD88'])
    else:
      continue
    
    measurements = []
    for datastream in sensor['Datastreams']:
      if datastream['name'] == 'Water Level':
        for observation in datastream['Observations']:
          date = observation['phenomenonTime'] # iso 8601 utc
          # print utc, dateutil.parser.parse(utc)
          value = observation['result'] + (elevation_navd88*3.28)

          measurement = {
            'date': date,
            'values': {
              'navd88': {
                'value': value,
                'unit': 'ft',
                'riskRating': 'Normal',
                'confidenceInterval': 0.5,
              }
            }
          }
          measurements.append(measurement)

    measurements.reverse() # sort ascending

    # TODO - helper function enriches the `measurements` array

    if measurements:
      d = {
        'id': _id,
        'name': name,
        'description': description,
        'lng': lng,
        'lat': lat,
        'measurements': measurements,
      }
      ret.append(d)

  return jsonify(ret)
  

if __name__ == "__main__":
  app.run(debug=True, port=5000)
