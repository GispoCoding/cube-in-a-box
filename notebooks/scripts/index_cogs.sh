#!/bin/sh

export STAC_API_URL='https://earth-search.aws.element84.com/v0/'
# Give extent in lon,lat
stac-to-dc --bbox="${EXTENT:-28,9,29,10}" --collections='sentinel-s2-l2a-cogs' \
--datetime="${TIMERANGE:-2020-10-01/2020-10-20}" s2_l2a
