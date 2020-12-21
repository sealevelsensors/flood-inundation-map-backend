import datetime
import dateutil.parser
from google.cloud import storage
import json
import requests
import time

import numpy as np

def get_sensor_measurements(sensor_id, start_iso, end_iso):
  """
  Get all measurements for sensor `sensor_id` between `start_date` and `end_date`
  """
  # _start = time.time()

  measurements = []

  sensor_url = 'https://api.sealevelsensors.org/v1.0/Things({0})?$expand=Datastreams/ObservedProperty,Datastreams/Sensor,Locations'.format(
      sensor_id)
  r = requests.get(sensor_url)
  sensor = r.json()

  measurements_url = ''
  for datastream in sensor['Datastreams']:
    if datastream['name'] == 'Water Level':
      measurements_url = '{0}/Observations'.format(datastream["@iot.selfLink"])
  if not measurements_url:
    return measurements

  next_url = '{0}?$filter=phenomenonTime ge {1} and phenomenonTime le {2}&$top=1000'.format(
      measurements_url, start_iso, end_iso)

  while next_url:
    r = requests.get(next_url)
    for m in r.json()['value']:
      measurement = {
          'date':
          m['phenomenonTime'],
          'water_level':
          round(
              ((float(sensor['properties']['elevationNAVD88']) + m['result']) *
               3.28), 2),
          'confidence_interval':
          0.6,
          'unit':
          'ft',
          'datum':
          'NAVD 88'
      }
      measurements.append(measurement)

    next_url = ''
    if '@iot.nextLink' in r.json():
      next_url = r.json()['@iot.nextLink']

  # sort by date in ascending order
  measurements = sorted(measurements,
                        key=lambda m: dateutil.parser.isoparse(m['date']),
                        reverse=False)

  # print 'Finished in ${0}s'.format(time.time() - _start)
  return measurements


def upload_blob(bucket_name, data, destination_blob_name):
  """Uploads a file to the bucket."""
  # bucket_name = "your-bucket-name"
  # data = stringified json
  # destination_blob_name = "storage-object-name"

  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(destination_blob_name)

  blob.upload_from_string(data, content_type='application/json')

  print("File uploaded to {}.".format(destination_blob_name))


def read_blob(bucket_name, source_blob_name):
  """Downloads a blob from the bucket as JSON."""

  storage_client = storage.Client()

  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(source_blob_name)

  ret = json.loads(blob.download_as_string())
  return ret


def conduct_objective_mapping(sensor_lon, sensor_lat, data, lon_gr, lat_gr, xcorr, ycorr, errcomp):
    """
    map water levels (data) measured at each sensor location (sensor_lon,sensor_lat)
    over the entire region (lon_gr,lat_gr)
    xcorr and ycorr define the spatial covariance between
    sensor locations and all locations throughout the region
    """

    I = len(data[:])
    datasize = I

    #     compute the spatial mean of the data by fitting a plane mean = a + b*lon + c*lat where
    #     [a,b,c]=m is the vector of the coefficients

    #     normalize the data by removing the spatial mean and store the mean value
    d, m = remove_global_mean(sensor_lon, sensor_lat, data)

    #     Compute Covariance Matrices using Gaussian for x and y correlation
    #     Set correlation scale for Covariance Matrix
    a = xcorr
    b = ycorr

    #     Scales are set according to Chereskin et al. JGR Vol. 101 pag. 22,619-22,629, OCT 15,1996

    #     Grid Covariance = GD
    GD = get_gaussian_covariance(sensor_lon, lon_gr, sensor_lat, lat_gr, a, b)

    #     Data Covariance = DD
    DD = get_gaussian_covariance(sensor_lon, sensor_lon, sensor_lat, sensor_lat, a, b)
    DD = DD + np.eye(datasize) * 0.01
    A = np.linalg.lstsq(DD.conj().T, GD)[0].conj().T
    d_gr = A @ d

    #     Set up bidimensional arrays for plotting
    field = d_gr

    [I, J] = lon_gr.shape
    field = field.reshape(I, J, order='F')

    #     utilize the spatial mean calculated by "remove_global_mean" to
    #     add the mean back to data to represent water level again
    mean_gr = add_global_mean(lon_gr, lat_gr, m)
    field = field + mean_gr

    #     compute error map of interpolated water levels (if requested by user) using the same gaussian covariane as an error function
    errmap = 0
    if errcomp == 1:
        #     Compute error map
        #     Grid - Grid Covariance = GG
        GG = get_gaussian_covariance(lon_gr, lon_gr, lat_gr, lat_gr, a, b)
        E = GG - A @ GD
        dE = np.diag(E)
        errmap = dE.reshape(I, J, order='F')

    return field, errmap


def get_gaussian_covariance(x, x1, y, y1, a, b):
    """
    Build gaussian covariance using the function: exp(- ((x-x1)/a)^2 - ((y-y1)/b)^2)
    """
    i = x.size
    j = x1.size

    # reshape and map the arrays
    X = map_to_2D(x, j)
    Y = map_to_2D(y, j)
    X1 = map_to_2D(x1, i).conj().T
    Y1 = map_to_2D(y1, i).conj().T

    GaCOV = (((X - X1) * (X - X1)) * (1 / (a * a))) + (((Y - Y1) * (Y - Y1)) * (1 / (b * b)))
    GaCOV = np.exp(-GaCOV)

    return GaCOV


def map_to_2D(x, J):
    """
    helper function for Gaussian Covariance
    flatten array into 1D vector and map over a 2D matrix
    """
    I = x.size
    X = np.zeros((I, 1))
    X[:, 0] = x.flatten(order='F')
    X = np.tile(X, (1, J))

    return X


def remove_global_mean(sensor_lon, sensor_lat, data):
    """
    compute the spatial mean of the data by fitting a plane mean = a + b*lon + c*lat
    where [a,b,c] = m is the vector of the coefficients
    subtract the mean from the data to return a normalized version of the data
    """

    # find and remove NaNs from the data, lat, and lon
    nan_indices = np.where(~np.isnan(data))[0]
    data = data[nan_indices]
    sensor_lon = sensor_lon[0][nan_indices]
    sensor_lat = sensor_lat[0][nan_indices]

    #     set up G as matrix with lon,lat = [1]
    I = data.size
    G = np.zeros((I, 3))
    G[:, 0] = sensor_lon[:]
    G[:, 1] = sensor_lat[:]
    G[:, 2] = 1

    #     set up d as data matrix
    d = data[:]

    # compute spatial mean
    CME = G.conj().T @ G + np.diag([1, 1, 1]) * 0.0001

    A = np.linalg.inv(CME)
    b = G.conj().T @ d
    m = A @ b
    d = G @ m

    # subtract mean from data
    d = data - d

    return d, m


def add_global_mean(lon_gr, lat_gr, m):
    """
    add back the spatial mean of the data
    """
    Ihat = len(lon_gr.flatten(order='F'))
    G = np.zeros((Ihat, 3))
    G[:, 0] = lon_gr.flatten(order='F')
    G[:, 1] = lat_gr.flatten(order='F')
    G[:, 2] = 1

    #     reshape the mean to the larger grid size
    mean_gr = G @ m
    [I, J] = lon_gr.shape
    mean_gr = mean_gr.reshape(I, J, order='F')
    return mean_gr

