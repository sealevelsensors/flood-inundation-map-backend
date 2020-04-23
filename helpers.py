import datetime
import dateutil.parser
from google.cloud import storage
import json
import requests
import time


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


def DoOA(lon_d, lat_d, data, lon_gr, lat_gr, xcorr, ycorr, errcomp):
    I = len(data[:])
    datasize = I

    #     compute the spatial mean of the data by fitting a plane mean = a + b*lon + c*lat where
    #     [a,b,c]=m is the vector of the coefficients
    d, m = RemoveGlobalMean(lon_d, lat_d, data)

    #     Compute Covariance Matrices using Gaussian ofr x and y correlation
    #     Set correlation scale for Covariance Matrix
    a = xcorr
    b = ycorr

    #     Scales are set same as Chereskin et al. JGR Vol. 101 pag. 22,619-22,629, OCT 15,1996

    #     Data - Grid Covariance = GD
    GD = GaussianCovariance(lon_d, lon_gr, lat_d, lat_gr, a, b)

    #     Data - Data Covariance = DD
    DD = GaussianCovariance(lon_d, lon_d, lat_d, lat_d, a, b)
    DD = DD + np.eye(datasize) * 0.01
    A = np.linalg.lstsq(DD.T, GD)[0].T
    d_gr = A @ d

    #     Setup bidimensional arrays for plotting
    field = d_gr

    [I, J] = lon_gr.shape
    field = field.reshape(I, J, order='F')

    #     Add the mean back
    mean_gr = AddGlobalMean(lon_gr, lat_gr, m)
    field = field + mean_gr

    errmap = 0
    if errcomp == 1:
        #     Compute error map
        #     Grid - Grid Covariance = GG
        GG = GaussianCovariance(lon_gr, lon_gr, lat_gr, lat_gr, a, b)
        E = GG - A @ GD
        dE = np.diag(E)
        errmap = dE.reshape(I, J, order='F')

    return field, errmap


# Build gaussian covariance
# exp(- ((x-x1)/a)^2 - ((y-y1)/b)^2)
def GaussianCovariance(x, x1, y, y1, a, b):
    i = x.size
    j = x1.size
    X = M2d(x, j)
    Y = M2d(y, j)
    X1 = M2d(x1, i).T
    Y1 = M2d(y1, i).T

    sio.savemat('X_test.mat', {'arr': X})
    sio.savemat('X1_test.mat', {'arr': X1})
    sio.savemat('Y_test.mat', {'arr': Y})
    sio.savemat('Y1_test.mat', {'arr': Y1})

    GaCOV = (((X - X1) * (X - X1)) * (1 / (a * a))) + (((Y - Y1) * (Y - Y1)) * (1 / (b * b)))
    GaCOV = np.exp(-GaCOV)

    return GaCOV


# helper function for Gaussian Covariance
def M2d(x, J):
    I = x.size
    X = np.zeros((I, 1))
    X[:, 0] = x.flatten(1)
    X = np.tile(X, (1, J))

    #     I=x.size
    #     X=np.zeros((I,1))
    #     X[:]=x[:]
    #     X=np.tile(X,(1,J))
    return X


# normalize the data by removing the mean
def RemoveGlobalMean(lon_d, lat_d, data):
    find = np.where(~np.isnan(data))[0]
    data = data[find]
    lon_d = lon_d[0][find]
    lat_d = lat_d[0][find]

    #     set up G
    I = data.size
    datasize = I
    G = np.zeros((I, 3))
    G[:, 0] = lon_d[:]
    G[:, 1] = lat_d[:]
    G[:, 2] = 1

    #     set up d
    d = data[:]

    CME = G.T @ G + np.diag([1, 1, 1]) * 0.0001
    CME.shape

    A = np.linalg.inv(CME)
    b = G.T @ d
    m = A @ b
    d = G @ m

    d = data - d

    return d, m


def AddGlobalMean(lon_gr, lat_gr, m):
    Ihat = len(lon_gr.flatten(1))
    G = 0
    G = np.zeros((Ihat, 3))
    G[:, 0] = lon_gr.flatten(1)
    G[:, 1] = lat_gr.flatten(1)
    G[:, 2] = 1

    mean_gr = G @ m
    [I, J] = lon_gr.shape
    mean_gr = mean_gr.reshape(I, J, order='F')
    return mean_gr
