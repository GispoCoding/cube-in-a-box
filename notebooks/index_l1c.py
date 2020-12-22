# GDAL v3.0.4
import os
import logging
from multiprocessing import current_process, Manager
from pathlib import Path
from queue import Empty
from typing import Dict

import boto3
from boto3.session import Session
from xml.etree import ElementTree
from hashlib import md5

import datacube
from datacube.index.hl import Doc2Dataset
from datacube.utils import changes

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
    obj_key = format_obj_key(obj_key)
    measurements = doc["measurements"]
    for measurement in measurements:
        measurements[measurement]["path"] = get_s3_url(bucket_name, obj_key + '/' + measurements[measurement]["path"])
    return doc


def format_s3_key(bucket_name: str, key: str) -> (str, str):
    key_path = Path(key)
    key_parts = list(key_path.parts)
    region_code = "".join(key_parts[1:4])
    uri = 's3://' + str(Path(bucket_name / key_path.parent))
    return uri, region_code


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
    uri, region_code = format_s3_key(bucket_name, key)

    tile_id = data.find("./*/TILE_ID").text
    sensing_time = data.find("./*/SENSING_TIME").text
    crs_code = data.find("./*/Tile_Geocoding/HORIZONTAL_CS_CODE").text.lower()
    sun_zenith = float(data.find("./*/Tile_Angles/Mean_Sun_Angle/ZENITH_ANGLE").text)
    sun_azimuth = float(data.find("./*/Tile_Angles/Mean_Sun_Angle/AZIMUTH_ANGLE").text)
    cloudy_pixel_percentage = float(data.find("./*/Image_Content_QI/CLOUDY_PIXEL_PERCENTAGE").text)
    grids = read_grid_metadata(data)

    l1c_measurements = ["B01_60m", "B02_10m", "B03_10m", "B04_10m", "B05_20m",
                        "B06_20m", "B07_20m", "B08_10m", "B09_60m", "B8A_20m",
                        "B10_60m", "B11_20m", "B12_20m"]
    eo3 = {
        "id": md5(uri.encode("utf-8")).hexdigest(),
        "$schema": "https://schemas.opendatacube.org/dataset",
        "product": {
            "name": "s2a_level1c_granule",
        },
        "crs": crs_code,
        "grids": {
            "default": {  # 10m
                "shape": [grids["10"]["nrows"], grids["10"]["ncols"]],
                "transform": grids["10"]["trans"],
            },
            "20m": {
                "shape": [grids["20"]["nrows"], grids["20"]["ncols"]],
                "transform": grids["20"]["trans"],
            },
            "60m": {
                "shape": [grids["60"]["nrows"], grids["60"]["ncols"]],
                "transform": grids["60"]["trans"],
            },
        },
        "measurements": {},
        "location": uri,
        "properties": {
            "tile_id": tile_id,
            "eo:instrument": "MSI",
            "eo:platform": "SENTINEL-2A",  # TODO: read A or B from metadata
            "odc:file_format": "JPEG2000",
            "datetime": sensing_time,
            "odc:region_code": region_code,
            "mean_sun_zenith": sun_zenith,
            "mean_sun_azimuth": sun_azimuth,
            "cloudy_pixel_percentage": cloudy_pixel_percentage,
        },
        "lineage": {},
    }

    for measurement in l1c_measurements:
        band, resolution = measurement.split("_")
        eo3["measurements"][band] = {"path": f"{band}.jp2"}
        if resolution == "10m":
            grid = "default"
        else:
            grid = resolution
        eo3["measurements"][band]["grid"] = grid

    return absolutify_s3_paths(eo3, bucket_name, key)


def read_grid_metadata(data):
    grids = {
        "10": {},
        "20": {},
        "60": {},
    }
    for resolution in grids:
        grids[resolution]["nrows"] = int(data.findall(
            f"./*/Tile_Geocoding/Size[@resolution='{resolution}']/NROWS")[0].text)
        grids[resolution]["ncols"] = int(data.findall(
            f"./*/Tile_Geocoding/Size[@resolution='{resolution}']/NCOLS")[0].text)
        grids[resolution]["ulx"] = float(data.findall(
            f"./*/Tile_Geocoding/Geoposition[@resolution='{resolution}']/ULX")[0].text)
        grids[resolution]["uly"] = float(data.findall(
            f"./*/Tile_Geocoding/Geoposition[@resolution='{resolution}']/ULY")[0].text)
        grids[resolution]["xdim"] = float(data.findall(
            f"./*/Tile_Geocoding/Geoposition[@resolution='{resolution}']/XDIM")[0].text)
        grids[resolution]["ydim"] = float(data.findall(
            f"./*/Tile_Geocoding/Geoposition[@resolution='{resolution}']/YDIM")[0].text)
        grids[resolution]["trans"] = [grids[resolution]["xdim"], 0.0, grids[resolution]["ulx"], 0.0,
                                      grids[resolution]["ydim"], grids[resolution]["uly"],
                                      0.0, 0.0, 1.0]
    return grids


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
            uri = format_s3_key(bucket_name, key)[0]
            add_dataset(dataset_doc, uri, index)
            queue.task_done()
        except Empty:
            break
        except EOFError:
            break


def main():
    # prepare queue
    manager = Manager()
    queue = manager.Queue()

    session = boto3.Session(
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name='eu-central-1',
    )
    s3 = session.resource('s3')
    bucket = s3.Bucket(L1C_BUCKET)

    # TODO: index other areas
    for obj in bucket.objects.filter(Prefix='tiles/35/P/PM/2020/10/', RequestPayer='requester'):
        if obj.key.endswith('metadata.xml'):
            queue.put(obj.key)
    q_size = queue.qsize()
    print(f"indexing {q_size} tiles")
    queue.put(GUARDIAN)

    worker(session, L1C_BUCKET, queue)  # TODO: multithread if necessary
    print(f"finished indexing {q_size} tiles")

if __name__ == "__main__":
    main()
