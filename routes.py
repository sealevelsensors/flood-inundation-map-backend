from flask import Flask, jsonify
from flask_cors import CORS
import datetime
import dateutil.parser
import requests
import requests_toolbelt.adapters.appengine

# Use the App Engine Requests adapter. This makes sure that Requests uses
# URLFetch.
requests_toolbelt.adapters.appengine.monkeypatch()

app = Flask(__name__)
CORS(app)


@app.route("/sensors")
def sensors():

  # get all sensors
  sensors_url = 'https://api.sealevelsensors.org/v1.0/Things?$expand=Datastreams/ObservedProperty,Datastreams/Sensor,Locations'
  r = requests.get(sensors_url)
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
      # get all water level measurements for the last n days
      NUM_DAYS_AGO = 2
      today = datetime.datetime.utcnow()
      n_days_ago = today - datetime.timedelta(days=NUM_DAYS_AGO)

      if datastream['name'] == 'Water Level':
        observations_url = '{0}/Observations?$filter=phenomenonTime ge {1} and phenomenonTime le {2}&$top=1000'.format(
            datastream["@iot.selfLink"],
            n_days_ago.strftime("%Y-%m-%dT%H:%M:%SZ"),
            today.strftime("%Y-%m-%dT%H:%M:%SZ"))
        r = requests.get(observations_url)
        observations = r.json()['value']

        for observation in observations:
          date = observation['phenomenonTime']  # iso 8601 utc
          value = (observation['result'] + elevation_navd88) * 3.28
          value = round(value, 2)

          measurement = {
              'date': date,
              'values': {
                  'navd88': {
                      'value': value,
                      'unit': 'ft',
                      'riskRating': 'Normal',
                      'confidenceInterval': 0.6,
                      'datum': 'NAVD 88'
                  }
              }
          }
          measurements.append(measurement)

    # sort by date in ascending order
    measurements = sorted(measurements,
                          key=lambda m: dateutil.parser.isoparse(m['date']),
                          reverse=False)

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
  app.run(debug=True, port=5001)
