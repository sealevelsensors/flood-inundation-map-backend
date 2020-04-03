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
