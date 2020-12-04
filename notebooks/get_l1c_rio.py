import os
import boto3
import rasterio as rio
from rasterio.session import AWSSession

# session = boto3.Session(
#     aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
#     aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
#     region_name='eu-central-1',
# )

# create AWS session object
# aws_session = AWSSession(session, requester_pays=True)

# with rio.Env(session=aws_session):
with rio.open("/vsis3/sentinel-s2-l1c/tiles/35/P/PM/2020/10/5/0/B10.jp2") as src:
    arr = src.read(1)

print(arr)
