#!/bin/sh

datacube -v system init
datacube product add https://raw.githubusercontent.com/digitalearthafrica/config/master/products/esa_s2_l2a.yaml
datacube product add ../products/s2_granules.yaml
