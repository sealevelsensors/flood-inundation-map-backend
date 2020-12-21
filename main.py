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
from scipy.interpolate import RectBivariateSpline

import rasterio
from rasterio.enums import Resampling

import helpers

app = Flask(__name__)
CORS(app)
os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/akhil/Documents/GCP/Post-Aug/Inundation/PYTHON/perceptive-bay-214919-8682c514b0a2.json"


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
    _start = time.time()

    # get all sensor IDs and [lat,lng] coordinates
    sensors = helpers.read_blob('perceptive-bay-214919.appspot.com',
                        'sensors.json')

    # gather lat,lon coordinates for each sensor installation
    sensor_lat = np.array([])
    sensor_lon = np.array([])
    for i in range(len(sensors)):
        sensor_lat = np.append(sensor_lat, sensors[i]["lat"])
        sensor_lon = np.append(sensor_lon, sensors[i]["lng"])

    # make a mesh grid throughout the sensor network region over which to interpolate the inundation layer
    # redefine xlim & ylim as sensor network expands
    delta_space = 0.01  # 0.01deg lat/lon is approximately 1km
    xlim = [-81.24, -80.82]
    ylim = [31.86, 32.08]
    # define number of points along lat and lon axes of grid by the interval delta_space
    lat_grid_length_coarse = round((ylim[1] - ylim[0]) / delta_space) + 1
    lon_grid_length_coarse = round((xlim[1] - xlim[0]) / delta_space) + 1
    # create the latitude and longitude grids
    lat_grid_coarse, lon_grid_coarse = np.meshgrid(np.linspace(31.86, 32.08, lat_grid_length_coarse),
                                                   np.linspace(-81.24, -80.82, lon_grid_length_coarse))

    # select a short period of time to use in objective mapping function for "live" updated inundation
    DELTA_MINUTES = 15
    # get current datetime in UTC (9 hours ahead of PST), in 24 hour format (11,12,13,14)
    end_datetime = datetime.datetime.utcnow()
    start_datetime = end_datetime - datetime.timedelta(minutes=DELTA_MINUTES)
    # format the datetimes
    start_iso = start_datetime.strftime('%Y-%m-%dT%H:%M:00Z')
    end_iso = end_datetime.strftime('%Y-%m-%dT%H:%M:00Z')

    # average the water levels measured by each sensor over DELTA_MINUTES
    avg_water_levels = np.array([])
    for sensor in sensors:
        sensor_id = sensor["id"]
        water_levels = np.array([])
        sensor_measurements = helpers.get_sensor_measurements(sensor_id, start_iso, end_iso)
        if sensor_measurements:
            for i in range(len(sensor_measurements)):
                water_levels = np.append(water_levels, sensor_measurements[i]["water_level"])
            avg_water_levels = np.append(avg_water_levels, water_levels.mean())
    avg_water_levels = avg_water_levels[np.newaxis].conj().T

    # filter out NaNs from avg_water_levels and apply filter to lat,lon
    nan_filter = np.where(~np.isnan(avg_water_levels))[0]
    avg_water_levels = avg_water_levels[nan_filter]
    sensor_lon = sensor_lon[nan_filter][np.newaxis]
    sensor_lat = sensor_lat[nan_filter][np.newaxis]

    """
    the decorrelation length scale was determined by estimating the spatial covariance between all sensor locations 
    as a function of distance. then a decorrelation length scale was emperically fit over the covariance estimates.

    the chosen decorrelation of 0.05 degrees is equivalent to approximately 5km, meaning that after exceeding a 
    5km distance from a sensor location, water levels are no longer correlated with that sensor location.
    although 5km gives less spatial coverage in mapping, it is more conservative in terms of error.

    this value can be reassessed as the sensor network grows in coverage and density.
    """

    # decorrelatiton length scale
    xcorr = 0.05  # 0.05deg lat/lon is approximately 5km
    ycorr = 0.05
    errcomp = 1  # 1 if errcomp is desired, 0 if errcomp is undesired
    water_level_layer_coarse, errmap_coarse = helpers.conduct_objective_mapping(sensor_lon, sensor_lat,
                                                                                avg_water_levels, lon_grid_coarse,
                                                                                lat_grid_coarse, xcorr, ycorr, errcomp)

    # Interpolate over a finer grid

    delta_space_fine = 0.0001  # 0.0001deg lat/lon is approximately 10m
    lat_grid_length_fine = round((32.08 - 31.86) / delta_space_fine) + 1
    lon_grid_length_fine = round((81.24 - 80.82) / delta_space_fine) + 1
    lat_grid_fine, lon_grid_fine = np.meshgrid(np.linspace(31.86, 32.08, lat_grid_length_fine),
                                               np.linspace(-81.24, -80.82, lon_grid_length_fine))

    # define interpolation function
    water_level_func = RectBivariateSpline(np.linspace(-81.24, -80.82, lon_grid_length_coarse),
                                           np.linspace(31.86, 32.08, lat_grid_length_coarse), water_level_layer_coarse)
    error_function = RectBivariateSpline(np.linspace(-81.24, -80.82, lon_grid_length_coarse),
                                         np.linspace(31.86, 32.08, lat_grid_length_coarse), errmap_coarse)

    # input lat and lon vectors with finer spacing into interpolation functions to create water level layer and error map with finer grid
    water_level_layer_fine_intermediate = water_level_func(np.linspace(-81.24, -80.82, lon_grid_length_fine),
                                                           np.linspace(31.86, 32.08, lat_grid_length_fine))
    errmap_fine = error_function(np.linspace(-81.24, -80.82, lon_grid_length_fine),
                                 np.linspace(31.86, 32.08, lat_grid_length_fine))

    # filter out interpolated water levels where gaussian error function was greater than or equal to 0.2 for a more conservative result
    high_error_mask_fine = errmap_fine.copy()
    high_error_mask_fine[:] = np.NaN
    high_error_mask_fine[errmap_fine < 0.2] = 1
    water_level_layer_fine = water_level_layer_fine_intermediate * high_error_mask_fine


    # read Chatham County LiDAR DEM 1m res, pre-clippped in ArcGIS to match extent of inundation bounds, defined above
    # lidar_clippedDEM_path = helpers.read_blob('perceptive-bay-214919.appspot.com',
    #                     'DEM_1M_2009_clippedExtent.tif')
    lidar_clippedDEM_path = '/Users/akhil/Documents/GCP/Post-Aug/Inundation/PYTHON/DEM_1M_2009/DEM_1M_2009_clippedExtent.tif'

    with rasterio.open(lidar_clippedDEM_path) as dataset:

        # resample data to target shape
        data = dataset.read(
            out_shape=(
                dataset.count,
                int(water_level_layer_fine.shape[-1]),
                int(water_level_layer_fine.shape[-2])
            ),
            resampling=Resampling.bilinear
        )

        # resample mask - mask is built into tif file to show which points are "no data" values
        mask_resampled = dataset.read_masks(
            out_shape=(
                dataset.count,
                int(water_level_layer_fine.shape[-1]),
                int(water_level_layer_fine.shape[-2])
            ),
            resampling=Resampling.bilinear
        )

        # calculating inundation profile and filling NaNs from Water Level layer and DEM

        # transpose and flip water level layer to match DEM orientation - different packages and file types read lat,lon in different orders
        water_level_oriented = np.flipud(water_level_layer_fine.conj().T)
        # subtracting DEM from interpolated water level
        inundation_layer = water_level_oriented - data[0]
        # using the mask to set the inundation layer to NaN where the DEM had "no data"
        inundation_layer[mask_resampled[0] == 0] = np.NaN
        # setting inundation layer to NaN where water level layer had NaN
        inundation_layer[np.isnan(water_level_oriented)] = np.NaN
        # in case there are any negative values left in the the inundation layer, setting those to NaN
        inundation_layer[inundation_layer < 0] = np.NaN

        # writing inundation_layer to json

        # orient inundation profile to match orientation of lat and lon arrays - first flip then transpose (opposite of previous orientation)
        inundation_layer_oriented = np.flipud(inundation_layer.conj()).T

        seq = []
        for i in range(lon_grid_fine.shape[0]):
            for j in range(lon_grid_fine.shape[1]):
                if (~np.isnan(inundation_layer_oriented[i][j])):
                    d = {
                        'COORDINATES': [lon_grid_fine[i][j], lat_grid_fine[i][j]],
                        'INTENSITY': inundation_layer_oriented[i][j] / 10, # set to max 10ft of inundation
                        'INUNDATION_DEPTH': inundation_layer_oriented[i][j]
                    }
                    seq.append(d)

        # upload to storage
        helpers.upload_blob('perceptive-bay-214919.appspot.com', json.dumps(seq),
                            'inundation.json')

    print('[_tasks_inundation] Finished in ${0}s'.format(time.time() - _start))

    return 'success'


if __name__ == "__main__":
  # To run: `dev_appserver.py --application=perceptive-bay-214919 app.yaml --support_datastore_emulator=False`
  app.run(debug=True)
