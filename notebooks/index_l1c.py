# GDAL v3.0.4
import os
import logging
from multiprocessing import Process, current_process, Manager, cpu_count
from pathlib import Path
from queue import Empty
from typing import Dict

import boto3
from boto3.session import Session
import click
from xml.etree import ElementTree
from hashlib import md5

import datacube
from datacube.index.hl import Doc2Dataset
from datacube.utils import changes, aws

GUARDIAN = "GUARDIAN_QUEUE_EMPTY"
L1C_BUCKET = "sentinel-s2-l1c"
L2A_BUCKET = "sentinel-s2-l2a"

def _parse_value(s):
    s = s.strip('"')
    for parser in [int, float]:
        try:
            return parser(s)
        except ValueError:
            pass
    return s


def format_obj_key(obj_key):
    obj_key = '/'.join(obj_key.split("/")[:-1])
    return obj_key


def get_s3_url(bucket_name: str, obj_key: str):
    return f"http://{bucket_name}.s3.amazonaws.com/{obj_key}"


def absolutify_paths(doc, bucket_name, obj_key):
    objt_key = format_obj_key(obj_key)
    measurements = doc["measurements"]
    for measurement in measurements:
        measurements[measurement]["path"] = get_s3_url(bucket_name, objt_key + '/' + measurements[measurement]["path"])
    return doc

def format_s3_key(bucket_name: str, key: str) -> (str, str):
    keypath = Path(key)
    keyparts = list(keypath.parts)
    regioncode = "".join(keyparts[1:4])
    uri = 's3://' + str(Path(bucket_name / keypath.parent))
    return uri, regioncode

def absolutify_s3_paths(doc: Dict, bucket_name: str, key: str) -> Dict:
    measurements = doc["measurements"]
    uri = format_s3_key(bucket_name, key)[0]
    for measurement in measurements:
        measurement_key = uri + "/" + measurements[measurement]["path"]
        measurements[measurement]["path"] = measurement_key
    return doc

def add_dataset(doc, uri, index: datacube.index.index.Index, **kwargs):
    logging.info("Indexing %s", uri)
    resolver = Doc2Dataset(index, **kwargs)
    dataset, err = resolver(doc, uri)
    if err is not None:
        logging.error("%s", err)
        return dataset, err
    try:
        index.datasets.add(dataset)  # Source policy to be checked in sentinel 2 datase types
    except changes.DocumentMismatchError:
        index.datasets.update(dataset, {tuple(): changes.allow_any})
    except Exception as e:
        err = e
        logging.error("Unhandled exception %s", e)

    return dataset, err

def generate_eo3_dataset_doc(bucket_name: str, key: str, data: ElementTree) -> dict:
    """ Ref: https://datacube-core.readthedocs.io/en/latest/ops/dataset_documents.html """
    uri, regioncode = format_s3_key(bucket_name, key)
#     keypath = Path(key)
#     keyparts = list(keypath.parts)
#     regioncode = "".join(keyparts[1:4])
#     uri = 's3://' + str(Path(bucket_name / keypath.parent))

    sensing_time = data.findall("./*/SENSING_TIME")[0].text
    crs_code = data.findall("./*/Tile_Geocoding/HORIZONTAL_CS_CODE")[0].text.lower()
    product_names = {
        L1C_BUCKET: "s2a_level1c_granule",
        L2A_BUCKET: "s2a_sen2cor_granule"
    }

    nrows_10 = int(data.findall("./*/Tile_Geocoding/Size[@resolution='10']/NROWS")[0].text)
    ncols_10 = int(data.findall("./*/Tile_Geocoding/Size[@resolution='10']/NCOLS")[0].text)
    ulx_10 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='10']/ULX")[0].text)
    uly_10 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='10']/ULY")[0].text)
    xdim_10 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='10']/XDIM")[0].text)
    ydim_10 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='10']/YDIM")[0].text)
    trans_10 = [xdim_10, 0.0, ulx_10, 0.0, ydim_10, uly_10, 0.0, 0.0, 1.0]

    nrows_20 = int(data.findall("./*/Tile_Geocoding/Size[@resolution='20']/NROWS")[0].text)
    ncols_20 = int(data.findall("./*/Tile_Geocoding/Size[@resolution='20']/NCOLS")[0].text)
    ulx_20 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='20']/ULX")[0].text)
    uly_20 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='20']/ULY")[0].text)
    xdim_20 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='20']/XDIM")[0].text)
    ydim_20 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='20']/YDIM")[0].text)
    trans_20 = [xdim_20, 0.0, ulx_20, 0.0, ydim_20, uly_20, 0.0, 0.0, 1.0]

    nrows_60 = int(data.findall("./*/Tile_Geocoding/Size[@resolution='60']/NROWS")[0].text)
    ncols_60 = int(data.findall("./*/Tile_Geocoding/Size[@resolution='60']/NCOLS")[0].text)
    ulx_60 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='60']/ULX")[0].text)
    uly_60 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='60']/ULY")[0].text)
    xdim_60 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='60']/XDIM")[0].text)
    ydim_60 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='60']/YDIM")[0].text)
    trans_60 = [xdim_60, 0.0, ulx_60, 0.0, ydim_60, uly_60, 0.0, 0.0, 1.0]

    # l1c
#     ten_list = ['B02_10m', 'B03_10m', 'B04_10m', 'B08_10m']
#     twenty_list = ['B05_20m', 'B06_20m', 'B07_20m', 'B11_20m', 'B12_20m', 'B8A_20m',
#                    'B02_20m', 'B03_20m', 'B04_20m', 'B08_20m']
#     sixty_list = ['B01_60m', 'B02_60m', 'B03_60m', 'B04_60m', 'B8A_60m', 'B09_60m',
#                   'B05_60m', 'B06_60m', 'B07_60m', 'B11_60m', 'B12_60m', 'B10_60m', 'B08_60m']

    measurement_list = ["B01_60m", "B02_10m", "B03_10m", "B04_10m", "B05_20m",
                        "B06_20m", "B07_20m", "B08_10m", "B09_60m", "B8A_20m",
                        "B10_60m", "B11_20m", "B12_20m"]

    # l2a
    #ten_list = ['B02_10m', 'B03_10m', 'B04_10m', 'B08_10m']
    #twenty_list = ['B05_20m', 'B06_20m', 'B07_20m', 'B11_20m', 'B12_20m', 'B8A_20m',
    #               'B02_20m', 'B03_20m', 'B04_20m', B08_20m]
    #sixty_list = ['B01_60m', 'B02_60m', 'B03_60m', 'B04_60m', 'B8A_60m', 'B09_60m',
    #              'B05_60m', 'B06_60m', 'B07_60m', 'B11_60m', 'B12_60m']

    eo3 = {
        "id": md5(uri.encode("utf-8")).hexdigest(),
        "$schema": "https://schemas.opendatacube.org/dataset",
        "product": {
            #"name": product_names[bucket_name],
            "name": "s2a_level1c_granule",
        },
        "crs": crs_code,
        "grids": {
            "default": {
                "shape": [nrows_10, ncols_10],
                "transform": trans_10,
            },
            "20m": {
                "shape": [nrows_20, ncols_20],
                "transform": trans_20,
            },
            "60m": {
                "shape": [nrows_60, ncols_60],
                "transform": trans_60,
            },
        },
        "measurements": {},
        "location": uri,
        "properties": {  # TODO: add cloud cover from metadata
            "eo:instrument": "MSI",
            "eo:platform": "SENTINEL-2A",  # TODO: read A or B from metadata
            "datetime": sensing_time,
            "odc:file_format": "JPEG2000",  # TODO: check validity
            "odc:region_code": regioncode,
        },
        "lineage": {},
    }

    for measurement in measurement_list:
        band, resolution = measurement.split("_")
        eo3["measurements"][band] = {"path": f"{band}.jp2"}
        if resolution == '10m':
            grid = 'default'
        else:
            grid = resolution
        eo3["measurements"][band]["grid"] = grid

#     for measurement in ten_list:
#         band, resolution = measurement.split("_")
#         eo3["measurements"][measurement] = {"path": f"R{resolution}/{band}.jp2"}
#     for measurement in twenty_list:
#         band, resolution = measurement.split("_")
#         eo3["measurements"][measurement] = {
#             "path": f"R{resolution}/{band}.jp2",
#             "grid": "20m",
#         }
#     for measurement in sixty_list:
#         band, resolution = measurement.split("_")
#         eo3["measurements"][measurement] = {
#             "path": f"R{resolution}/{band}.jp2",
#             "grid": "60m",
#         }
#     eo3["measurements"]["SCL_20m"] = {
#         "path": "R20m/SCL.jp2",
#         "grid": "20m",
#     }
    return absolutify_s3_paths(eo3, bucket_name, key)

def worker(session: Session, bucket_name: str, queue, config=None):
    dc = datacube.Datacube(config=config)
    index = dc.index
    s3 = session.resource("s3")

    while True:
        try:
            key = queue.get(timeout=60)
            if key == GUARDIAN:
                break
            print(f"Processing {key} {current_process()}")
            obj = s3.Object(bucket_name, key).get(ResponseCacheControl="no-cache", RequestPayer="requester")
            raw = obj["Body"].read()
            content = str(raw, "utf-8")
            data = ElementTree.fromstring(content)
            dataset_doc = generate_eo3_dataset_doc(bucket_name, key, data)
            #uri = get_s3_url(bucket_name, key)
            #uri = 's3://' + str(Path(bucket_name / Path(key).parent))
            uri = format_s3_key(bucket_name, key)[0]
            add_dataset(dataset_doc, uri, index)
            queue.task_done()
        except Empty:
            break
        except EOFError:
            break

# prepare q
manager = Manager()
queue = manager.Queue()

session = boto3.Session(
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    region_name='eu-central-1',
)
s3 = session.resource('s3')
bucket = s3.Bucket(L1C_BUCKET)

for obj in bucket.objects.filter(Prefix='tiles/35/P/PM/2020/10/', RequestPayer='requester'):
    if obj.key.endswith('metadata.xml'):
        queue.put(obj.key)

queue.put(GUARDIAN)

worker(session, L1C_BUCKET, queue)

print("finished")
