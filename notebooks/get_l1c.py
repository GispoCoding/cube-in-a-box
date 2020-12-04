import datacube

# Create a query object
lat, lon = 10.821, 28.518
buffer = 0.01

query = {
    'time': ('2020-10-01', '2020-10-5'),
    'x': (lon - buffer, lon + buffer),
    'y': (lat + buffer, lat - buffer),
    'output_crs': 'epsg:32635',
    'resolution':(-20,20),
}

# Create a query object
lat, lon = 10.821, 28.518
buffer = 0.01

query = {
    'time': ('2020-10-01', '2020-10-5'),
    'x': (lon - buffer, lon + buffer),
    'y': (lat + buffer, lat - buffer),
    'output_crs': 'epsg:32635',
    'resolution':(-20,20),
}

dc = datacube.Datacube(app="s2-l1c-eu")
ds = dc.load(product='s2a_level1c_granule',
             dask_chunks={},
             **query)

arr = ds.B06.values
print(arr)
