from flask import Flask, jsonify
from flask_cors import CORS
import datetime
import dateutil.parser
import requests
import requests_toolbelt.adapters.appengine
import json
import time

# Use the App Engine Requests adapter. This makes sure that Requests uses
# URLFetch.
requests_toolbelt.adapters.appengine.monkeypatch()

app = Flask(__name__)
CORS(app)


@app.route("/sensors")
def sensors():
  start = time.time()

  f = open('output/sensors.json')
  ret = json.loads(f.read())
  f.close()

  print 'Finished in ${0}s'.format(time.time() - start)
  return jsonify(ret)


@app.route("/bridge-status")
def bridges():
  start = time.time()

  f = open('output/bridge-status.json')
  ret = json.loads(f.read())
  f.close()

  print 'Finished in ${0}s'.format(time.time() - start)
  return jsonify(ret)


# -----------------------------------------------------------------------------


@app.route("/tasks/sensors")
def tasks_sensors():
  start = time.time()

  NUM_DAYS_AGO = 2

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

    d = {
        'id': _id,
        'name': name,
        'description': description,
        'lng': lng,
        'lat': lat,
        'measurements': measurements,
    }
    ret.append(d)

  f = open('output/sensors.json', 'w')
  f.write(json.dumps(ret))
  f.close()

  print 'Finished in ${0}s'.format(time.time() - start)
  return 'Success'


@app.route("/tasks/bridge-status")
def tasks_bridge_status():
  start = time.time()

  NUM_DAYS_AGO = 61
  TODAY = datetime.datetime.utcnow()
  N_DAYS_AGO = TODAY - datetime.timedelta(days=NUM_DAYS_AGO)

  statuses = []

  f = open('input/bridges-with-sensors.json', 'r')
  data = json.loads(f.read())
  f.close()

  # get all sensors and bridges
  for item in data:
    # get sensor data
    sensor_id = item['@iot.id']
    sensor_name = item['name']
    sensor_description = item['description']
    sensor_elevation_navd88 = 0
    if 'elevationNAVD88' in item['properties']:
      sensor_elevation_navd88 = float(item['properties']['elevationNAVD88'])
    else:
      continue

    # get bridge data
    bridge_name = item['bridge']['name']
    bridge_elevation_navd88 = round((item['bridge']['elevationNAVD88'] * 3.28),
                                    2)
    bridge_description = item['bridge']['notes']

    events_dict = {}
    num_events = 0

    # get the highest point of the water for each day -------------------------
    # get all datastreams
    datastreams_url = item['Datastreams@iot.navigationLink']
    r = requests.get(datastreams_url)
    datastreams = r.json()['value']
    for datastream in datastreams:
      if datastream['name'] == 'Water Level':
        # get all water level measurements for the last 60 days
        next_url = '{0}/Observations?$filter=phenomenonTime ge {1} and phenomenonTime le {2}&$top=1000'.format(
            datastream['@iot.selfLink'],
            N_DAYS_AGO.strftime('%Y-%m-%dT00:00:00Z'),
            TODAY.strftime('%Y-%m-%dT%H:%M:%SZ'))
        r = requests.get(next_url)

        while next_url:
          r = requests.get(next_url)
          measurements = r.json()['value']
          sorted_measurements = sorted(measurements,
                                       key=lambda k: k['@iot.id'])

          # for each day, get the highest point of the water
          for measurement in sorted_measurements:
            _datetime = measurement['phenomenonTime']
            date = _datetime.split('T')[0]

            value = (measurement['result'] + sensor_elevation_navd88) * 3.28
            rounded_value = round(value, 2)

            # get the max value
            if date in events_dict:
              if value >= events_dict[date]['maxWaterLevel']:
                events_dict[date] = {
                    'date': _datetime,
                    'maxWaterLevel': rounded_value,
                    'unit': 'ft',
                    'datum': 'NAVD 88'
                }
            else:
              events_dict[date] = {
                  'date': _datetime,
                  'maxWaterLevel': value,
                  'unit': 'ft',
                  'datum': 'NAVD 88'
              }

          next_url = ''
          if '@iot.nextLink' in r.json():
            next_url = r.json()['@iot.nextLink']

    # compute how far away water is from the bottom of the bridge -------------
    for date, value in events_dict.items():
      max_water_level = value['maxWaterLevel']

      distance = bridge_elevation_navd88 - max_water_level

      threshold_color = ''
      if distance <= 0.5:
        threshold_color = '#ff3300'  # red - <= 6in away from bottom
        num_events += 1
      elif distance > 0.5 and distance <= 1:
        threshold_color = '#ff9800'  # yellow - between 6in to 1ft away from bottom
        num_events += 1
      elif distance > 1:
        threshold_color = '#0da960'  # green - more than 1ft away from bottom

      events_dict[date]['distanceFromBridge'] = round(distance, 2)
      events_dict[date]['thresholdColor'] = threshold_color

    if num_events > 0:
      events_list = []
      for date in events_dict:
        events_list.append(events_dict[date])
      sorted_events_list = sorted(events_list, key=lambda k: k['date'])

      status = {
          'sensor': {
              '@iot.id': sensor_id,
              'name': sensor_name,
              'description': sensor_description,
              'elevationNAVD88': sensor_elevation_navd88
              # 'lat': 0,
              # 'lng': 0,
          },
          'bridge': {
              'name': bridge_name,
              'notes': bridge_description,
              'elevationNAVD88': bridge_elevation_navd88,
          },
          'numEvents': num_events,
          'events': sorted_events_list
      }

      statuses.append(status)

  # sort ascending by most number of events
  sorted_statuses = sorted(statuses, key=lambda k: k['numEvents'])
  f = open('output/bridge-status.json', 'w')
  f.write(json.dumps(sorted_statuses, indent=2))
  f.close()

  print 'Finished in ${0}s'.format(time.time() - start)
  return 'Success'


if __name__ == "__main__":
  app.run(debug=True, port=5001)