from flask import Flask, url_for, jsonify, request
from flask_cors import CORS
import datetime
import dateutil.parser
from google.cloud import storage
import json
import os
import requests
import time

import numpy as np

import helpers

app = Flask(__name__)
CORS(app)
# os.environ[
#     "GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/lpolepeddi/Documents/gcp/sea-level-sensors/cema-portal/perceptive-bay-214919-8682c514b0a2.json"


@app.route("/v1/layers")
def layers():
  # Display a list of all available layers
  ret = [{
      'id': 1,
      'name': 'sensors',
      'description': 'Sensors',
      'url': url_for('sensors', _external=True)
  }, {
      'id': 2,
      'name': 'bridges',
      'description': 'Bridges',
      'url': url_for('bridges', _external=True)
  }]

  return jsonify(ret)


@app.route("/v1/sensors")
def sensors():
  # Display a list of all sensors with water level measurements
  # query params: datum

  sensors = helpers.read_blob('perceptive-bay-214919.appspot.com',
                              'sensors.json')
  return jsonify(sensors)


@app.route("/v1/sensors/<int:sensor_id>")
def sensor(sensor_id):
  # Display a specific sensor
  # query params: start, end, datum

  _start = time.time()

  # make sure that the end_iso is the same as the end_iso of the latest_measurement in cached `sensors`
  sensors = helpers.read_blob('perceptive-bay-214919.appspot.com',
                              'sensors.json')
  end_iso = datetime.datetime.utcnow().strftime("%Y-%m-%dT23:59:59Z")
  for sensor in sensors:
    if sensor_id == sensor['id']:
      latest_measurement = sensor['latest_measurement']
      if latest_measurement:
        end_iso = latest_measurement['date']
  start_iso = (datetime.datetime.utcnow() -
               datetime.timedelta(days=2)).strftime("%Y-%m-%dT00:00:00Z")

  start = request.args.get('start')
  end = request.args.get('end')
  if start and end:
    start_iso = "{0}T00:00:00Z".format(start)
    end_iso = "{0}T23:59:59Z".format(end)

  ret = {}

  sensor_url = 'https://api.sealevelsensors.org/v1.0/Things({0})?$expand=Datastreams/ObservedProperty,Datastreams/Sensor,Locations'.format(
      sensor_id)
  r = requests.get(sensor_url)
  sensor = r.json()

  _id = sensor['@iot.id']
  name = sensor['name']
  description = sensor['description']
  lng, lat = sensor['Locations'][0]['location']['coordinates']

  elevation_navd88 = 0
  if 'elevationNAVD88' in sensor['properties']:
    elevation_navd88 = round(
        (float(sensor['properties']['elevationNAVD88']) * 3.28), 2)
  else:
    return jsonify(ret)

  url = url_for('sensor', sensor_id=_id, _external=True)
  things_url = sensor['@iot.selfLink']

  measurements_url = ''
  for datastream in sensor['Datastreams']:
    if datastream['name'] == 'Water Level':
      measurements_url = '{0}/Observations'.format(datastream["@iot.selfLink"])
  if not measurements_url:
    return jsonify(ret)

  measurements = helpers.get_sensor_measurements(_id, start_iso, end_iso)

  ret = {
      'id': _id,
      'name': name,
      'description': description,
      'lat': lat,
      'lng': lng,
      'elevation': {
          'elevation': elevation_navd88,
          'unit': 'ft',
          'datum': 'NAVD 88'
      },
      'url': url,
      'things_url': things_url,
      'measurements_url': measurements_url,
      'measurements': measurements
  }

  print('[sensor] Finished in ${0}s'.format(time.time() - _start))
  return jsonify(ret)


@app.route("/v1/bridges")
def bridges():
  # query params: datum

  bridges = helpers.read_blob('perceptive-bay-214919.appspot.com',
                              'bridges.json')
  return jsonify(bridges)


@app.route("/_tasks/sensors")
def _tasks_sensors():
  _start = time.time()

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
      elevation_navd88 = round(
          (float(sensor['properties']['elevationNAVD88']) * 3.28), 2)
    else:
      continue

    url = url_for('sensor', sensor_id=_id, _external=True)
    things_url = sensor['@iot.selfLink']

    measurements_url = ''
    for datastream in sensor['Datastreams']:
      if datastream['name'] == 'Water Level':
        measurements_url = '{0}/Observations'.format(
            datastream["@iot.selfLink"])
    if not measurements_url:
      continue

    end_iso = datetime.datetime.utcnow().strftime("%Y-%m-%dT23:59:59Z")
    start_iso = (datetime.datetime.utcnow() -
                 datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:00Z")
    all_measurements = helpers.get_sensor_measurements(_id, start_iso, end_iso)
    latest_measurement = {}
    if all_measurements:
      latest_measurement = all_measurements[-1]

    sensor = {
        'id': _id,
        'name': name,
        'description': description,
        'lat': lat,
        'lng': lng,
        'elevation': {
            'elevation': elevation_navd88,
            'unit': 'ft',
            'datum': 'NAVD 88'
        },
        'url': url,
        'things_url': things_url,
        'measurements_url': measurements_url,
        'latest_measurement': latest_measurement
    }

    ret.append(sensor)

  # upload to storage
  helpers.upload_blob('perceptive-bay-214919.appspot.com', json.dumps(ret),
                      'sensors.json')
  print('[_tasks_sensors] Finished in ${0}s'.format(time.time() - _start))

  return 'Success'


@app.route("/_tasks/bridges")
def _tasks_bridges():
  _start = time.time()

  f = open('input/bridges.json', 'r')
  bridges = json.loads(f.read())
  f.close()

  for bridge in bridges:
    elevation = bridge['elevation']['elevation']

    # get the bridge's associated sensor measurements for the last 60 days
    num_days = 61
    end_date = datetime.datetime.utcnow()
    start_date = end_date - datetime.timedelta(days=num_days)

    start_iso = start_date.strftime('%Y-%m-%dT00:00:00Z')
    end_iso = end_date.strftime('%Y-%m-%dT23:59:59Z')

    print('Getting measurements for {0}'.format(bridge["name"]))
    measurements = helpers.get_sensor_measurements(bridge["sensor_id"],
                                                   start_iso, end_iso)

    # for each day, get the highest point of the water
    max_water_levels = {}
    for measurement in measurements:
      _datetime = measurement['date']
      day = _datetime.split('T')[0]
      water_level = measurement['water_level']

      if day in max_water_levels:
        if water_level >= max_water_levels[day]['max_water_level']:
          max_water_levels[day] = {
              'date': _datetime,
              'max_water_level': water_level,
              'unit': 'ft',
              'datum': 'NAVD 88'
          }
      else:
        max_water_levels[day] = {
            'date': _datetime,
            'max_water_level': water_level,
            'unit': 'ft',
            'datum': 'NAVD 88'
        }

    # given the highest point of water, compute how far away
    # water is from the bottom of the bridge
    events = []
    num_events = 0
    for day in sorted(max_water_levels.keys()):
      d = max_water_levels[day]
      # compute how far away water is from the bottom of the bridge
      water_level = d['max_water_level']
      distance_from_bridge = round((elevation - water_level), 2)
      if distance_from_bridge <= 1:
        num_events += 1
      d['distance_from_bridge'] = distance_from_bridge
      events.append(d)

    bridge["events"] = events
    bridge["num_events"] = num_events

  # upload to storage
  helpers.upload_blob('perceptive-bay-214919.appspot.com', json.dumps(bridges),
                      'bridges.json')

  print('[_tasks_bridges] Finished in ${0}s'.format(time.time() - _start))

  return 'Success'

@app.route("/_tasks/inundation/")
def _tasks_inundation():
  # get all sensor IDs and [lat,lng] coordinates
  sensors_url = 'http://backend-dot-perceptive-bay-214919.appspot.com/v1/sensors'
  r = requests.get(sensors_url).json()
  sensorIDs = []
  lat_d = np.array([])
  lon_d = np.array([])
  for i in range(len(r)):
    sensorIDs.append(r[i]["id"])
    lat_d = np.append(lat_d,r[i]["lat"])
    lon_d = np.append(lon_d, r[i]["lng"])

  # make a mesh grid throughout the sensor network region over which to interpolate the inundation layer
  # redefine xlim & ylim based on sensor network
  delta_space = 0.01  # 0.01deg lat/lon is approximately 1km
  xlim = [-81.24, -80.82]
  ylim = [31.86, 32.08]
  num_lat = round((ylim[1] - ylim[0]) / delta_space) + 1
  num_lon = round((xlim[1] - xlim[0]) / delta_space) + 1
  lat_gr, lon_gr = np.meshgrid(np.linspace(31.86, 32.08, num_lat), np.linspace(-81.24, -80.82, num_lon))

  # average sensor measurements over a short period of time to use in objective mapping function
  DELTA_MINUTES = 30
  # get current datetime in UTC (9 hours ahead of PST), in 24 hour format (11,12,13,14)
  end_datetime = datetime.datetime.utcnow()
  start_datetime = end_datetime - datetime.timedelta(minutes=DELTA_MINUTES)
  # format the datetimes
  start_iso = start_datetime.strftime('%Y-%m-%dT%H:%M:00Z')
  end_iso = end_datetime.strftime('%Y-%m-%dT%H:%M:00Z')

  avgs = np.array([])
  for id in sensorIDs:
    water_levels = np.array([])
    meas = helpers.get_sensor_measurements(id, start_iso, end_iso)
    for i in range(len(meas)):
      water_levels = np.append(water_levels, meas[i]["water_level"] )
    avgs = np.append(avgs, water_levels.mean())
  avgs = avgs[np.newaxis].T

  # filter out NaNs from avgs and apply filter to lat,lon
  find = np.where(~np.isnan(avgs))[0]
  avgs = avgs[find]
  lon_d = lon_d[find][np.newaxis]
  lat_d = lat_d[find][np.newaxis]

  xcorr = 0.05
  ycorr = 0.05
  errcomp = 1
  # field, errmap = helpers.DoOA(lon_d, lat_d, avgs, lon_gr, lat_gr, xcorr, ycorr, errcomp)
  #
  # # Plot the map after removing regions of high error
  # mask = errmap.copy()
  # mask[:] = np.NaN
  # mask[errmap < 0.2] = 1
  #
  # final_layer = field*mask

  return str(avgs.shape)
  # return(json.dumps(meas))


if __name__ == "__main__":
  # To run: `dev_appserver.py --application=perceptive-bay-214919 app.yaml --support_datastore_emulator=False`
  app.run(debug=True)
