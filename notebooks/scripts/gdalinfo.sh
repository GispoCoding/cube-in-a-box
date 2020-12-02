AWS_ACCESS_KEY_ID=$AWS_SECRET_KEY_ID AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY AWS_REGION=eu-central-1 \
AWS_REQUEST_PAYER=requester gdalinfo /vsis3/sentinel-s2-l1c/tiles/35/P/PM/2020/10/10/0/B10.jp2
gdalinfo --version

# /vsis3 works
gdalinfo /vsis3/sentinel-cogs/sentinel-s2-l2a-cogs/6/W/XT/2020/10/S2B_6WXT_20201031_0_L2A/B01.tif

