# Calculating XTerrain for the Continental United States

```python
# Importing our libraries

import geopyspark as gps
import numpy as np
import pyproj
import fiona
import folium

from functools import partial
from shapely.geometry import Point
from shapely.ops import transform
from pyspark import SparkContext, StorageLevel
from pyspark.sql import SparkSession

# Setting up the SparkContext

conf = gps.geopyspark_conf(appName="geopython-notebook-emr", master='local[*]')
conf.set('spark.default.parallelism', 8)
conf.set('spark.ui.enabled', True)
conf.set('spark.yarn.executor.memoryOverhead', '1G')
conf.set('spark.yarn.driver.memoryOverhead', '1G')
conf.set('spark.master.memory', '9500M')
conf.set('spark.dynamicAllocation.enabled', True)
conf.set('spark.shuffle.service.enabled', True)
conf.set('spark.shuffle.compress', True)
conf.set('spark.shuffle.spill.compress', True)
conf.set('spark.rdd.compress', True)
conf.set('spark.driver.maxResultSize', '3G')
conf.set('spark.task.maxFailures', '33')
conf.set('spark.executor.extraJavaOptions', '-XX:+UseParallelGC')

sc = SparkContext(conf=conf)

hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
hadoopConf.set("fs.s3.awsAccessKeyId", '')
hadoopConf.set("fs.s3.awsSecretAccessKey", '')

pysc = gps.get_spark_context()
session = SparkSession.builder.config(conf=pysc.getConf()).enableHiveSupport().getOrCreate()
```

## Establishing Shared Values


```python
# The URI that the layers will be saved/read from

layer_uri = "s3://geopyspark-demo/geopython/catalog/emr"

# The AttributeStore for the URI

store = gps.AttributeStore(layer_uri)
```

## Creating a Friction Layer of the Continental United States

Friction Layer Factors:
 - Roads
 - Trails and Paths
 - Waterways
 - Land Cover
 - Elevation

 Data Sources:
  - Open Street Maps (OSM): Streets and waterways
  - National Land Cover Dataset (NLCD): Land cover
  - National Elevation Dataset (NED): Elevation

### Reading and Formatting the OSM Data


```python
# Read in the OSM data as an ORC file

file_uri = "s3://geotrellis-test/xterrain/continental-us.orc"
osm_dataframe = session.read.orc(file_uri)

# Get all of the lines that are contained within the DataFrame

osm = gps.vector_pipe.osm_reader.from_dataframe(osm_dataframe)
lines = osm.get_line_features_rdd()
```

#### Roads and Trails


```python
highways = lines.filter(lambda feature: 'highway' in feature.properties.tags)

path_tags = ['footway', 'steps', 'bridleway', 'path', 'cycleway', 'escalator']

# Filter out the highways into roads and paths

roads = highways.filter(lambda feature: feature.properties.tags['highway'] not in path_tags)
paths = highways.filter(lambda feature: feature.properties.tags['highway'] in path_tags)

# Encode the the paths with the default walking speed

path_features = paths\
    .map(lambda feature: gps.Feature(feature.geometry, gps.CellValue(3.74, 3.74)))\
    .persist(StorageLevel.MEMORY_AND_DISK_SER)
```

Now that we have our `path_features`, we can assign them `CellValue`s
based on the road's speed.

```python
# This logic assigns each section of road a
# speed based on the type of road that section is.

default_speeds = {
    'motorway': 65,
    'trunk': 45,
    'primary': 40,
    'secondary': 35,
    'tertiary': 30,
    'unclassified': 20,
    'residential': 20,
    'service': 15,
    'motorway_link': 45,
    'trunk_link': 40,
    'primary_link': 35,
    'secondary_link': 30,
    'tertiary_link': 25,
    'living_street': 5,
    'pedestrian': 5,
    'track': 15,
    'road': 30}

words = ['maxspeed', 'ambiguous', 'signals',
         'none', 'walk', 'variable',
         'national', 'fixme', 'unposted', 'implicit']

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def default_speed(highway):
    if not highway in default_speeds:
        return default_speeds['road']
    else:
        return default_speeds[highway]

def get_maxspeed(speed, units, highway):
    speeds = speed.split(';|,-')
    maxspeed = 0
    for sp in speeds:
        sp = sp.replace(units, '')
        if (is_number(sp)):
            if units == 'kph':
                sp = float(sp) / 1.609344
            elif units == 'knots':
                sp = 0.868976 * float(knots)
            else:
                sp = float(sp)

            if sp > maxspeed:
                maxspeed = sp
    if maxspeed > 0:
        speed = maxspeed
    else:
        speed = default_speed(highway)

    return speed

def get_highway_cellvalue(osm_feature):
    highway = osm_feature.properties.tags['highway']
    speed = osm_feature.properties.tags.get('maxspeed', '')

    speed = speed.lower().strip()

    # if we don't have a speed, give it a default
    if len(speed) == 0:
        speed = default_speed(highway)
    elif not is_number(speed):
        if 'kph' in speed:
            speed = get_maxspeed(speed, 'kph', highway)
        elif 'km/h' in speed:
            speed = get_maxspeed(speed, 'km/h', highway)
        elif 'kmh' in speed:
            speed = get_maxspeed(speed, 'kmh', highway)
        elif 'mph' in speed:
            speed = get_maxspeed(speed, 'mph', highway)
        elif 'knots' in speed:
            speed = get_maxspeed(speed, 'knots', highway)
        elif speed in words:
            speed = default_speed(highway)
        else:
            speed = get_maxspeed(speed, '', highway)
    if float(speed) <= 0.0:
        speed = default_speed(highway)

    speed = float(speed)
    return gps.CellValue(speed, speed)

# Encode the road speeds as feature properties for rasterization

road_features = roads\
    .map(lambda feature: gps.Feature(feature.geometry, get_highway_cellvalue(feature)))\
    .persist(StorageLevel.MEMORY_AND_DISK_SER)
```

#### Waterways


```python
waterways = lines.filter(lambda feature: 'waterway' in feature.properties.tags)

waterways_pmts_map = {
    'river': 0.3,
    'stream': 0.7,
    'brook': 0.8,
    'canal': 0.35,
    'drain': 0.85,
    'ditch': 0.8
}

def get_waterway_cellvalue(feature):
    waterway = feature.properties.tags['waterway']

    pmt = waterways_pmts_map.get(waterway)

    if pmt:
        value = pmt
    else:
        value = 0

    return gps.CellValue(value, value)

# Encode the water speeds as feature properties for rasterization

waterway_features = waterways\
    .map(lambda feature: gps.Feature(feature.geometry, get_waterway_cellvalue(feature)))\
    .persist(StorageLevel.MEMORY_AND_DISK_SER)
```

### Rasterizing OSM Features


```python
# Combine the roads, paths, and waterways into one RDD and then rasterize them

osm_raster = gps.geotrellis.rasterize_features(
    features = pysc.union([road_features, path_features, waterway_features]),
    crs = 4326,
    zoom = 10,
    cell_type=gps.CellType.INT8RAW,
    partition_strategy = gps.SpatialPartitionStrategy(1000))\
.convert_data_type(gps.CellType.FLOAT32, -2147483648.0)\
.persist(StorageLevel.MEMORY_AND_DISK_SER)
```

### Saving the Rasterized OSM Features


```python
tiled_osm = osm_raster.tile_to_layout(gps.GlobalLayout(), target_crs=3857).with_no_data(0.0)
osm_pyramid = tiled_osm.pyramid(partition_strategy=gps.SpatialPartitionStrategy(1000))

osm_layer_name = "rasterized-osm-features"

# Save layer histogram for later use

osm_hist = osm_pyramid.get_histogram()
store.layer(osm_layer_name).write("histogram", osm_hist.to_dict())

# Save layer pyramid

for zoom, layer in sorted(osm_pyramid.levels.items(), reverse=True):
    print("Writing zoom", zoom)
    store.layer(osm_layer_name, zoom).delete("metadata")
    gps.write(layer_uri, osm_layer_name, layer)
```

### Displaying the Rasterized OSM Features


```python
osm_layer_name = "rasterized-osm-features"
osm_hist = gps.Histogram.from_dict(store.layer(osm_layer_name).read("histogram"))
osm_color_map = gps.ColorMap.build(osm_hist, 'magma')

osm_tms = gps.TMS.build((layer_uri, osm_layer_name), osm_color_map)
osm_tms.bind("0.0.0.0", 56583)

osm_map = folium.Map()
folium.TileLayer(tiles='http://localhost:56583/tile/{z}/{x}/{y}.png',
                 attr="GeoPySpark").add_to(osm_map)

osm_map
```

### Reading and Formatting the NLCD Data


```python
# Reading NLCD Data

nlcd = gps.geotiff.get(
    gps.LayerType.SPATIAL,
    "s3://gt-rasters/nlcd/2011/tiles",
    crs="epsg:4326",
    max_tile_size=512,
    num_partitions=1000)

# Tiling it to the osm_raster's layout

tiled_nlcd = nlcd.tile_to_layout(
    osm_raster.layer_metadata,
    target_crs=4326,
    partition_strategy=gps.SpatialPartitionStrategy(1000))

# Reclassify the NLCD values based on estimated walking impact

nlcd_map = {
    11.0: 0.0,
    12.0: 0.15,
    21.0: 0.9,
    22.0: 0.9,
    23.0: 0.9,
    24.0: 0.95,
    31.0: 0.1,
    41.0: 0.7,
    42.0: 0.65,
    43.0: 0.75,
    51.0: 0.75,
    52.0: 0.75,
    71.0: 0.8,
    81.0: 0.8,
    82.0: 0.8,
    90.0: 0.2,
    95.0: 0.25
}

nlcd_pmts = tiled_nlcd\
    .convert_data_type(gps.CellType.FLOAT32, 0.0)\
    .reclassify(nlcd_map, float, gps.ClassificationStrategy.EXACT)
```

### Saving the Reclassified NLCD Layer


```python
nlcd_wm = nlcd_pmts.tile_to_layout(gps.GlobalLayout(), target_crs=3857)
nlcd_pyramid = nlcd_wm.pyramid(partition_strategy=gps.SpatialPartitionStrategy(1000))

nlcd_layer_name = "raclassified-nlcd"

# Save layer histogram for later use

nlcd_hist = nlcd_pyramid.get_histogram()
store.layer(nlcd_layer_name).write("histogram", nlcd_hist.to_dict())

# Save layer pyramid

for zoom, layer in sorted(nlcd_pyramid.levels.items(), reverse=True):
    print("Writing zoom", zoom)
    store.layer(nlcd_layer_name, zoom).delete("metadata")
    gps.write(layer_uri, nlcd_layer_name, layer)
```

### Displaying the Reclassified NLCD Data


```python
nlcd_layer_name = "raclassified-nlcd"
nlcd_hist = gps.Histogram.from_dict(store.layer(nlcd_layer_name).read("histogram"))
nlcd_color_map = gps.ColorMap.build(nlcd_hist, 'magma')

nlcd_tms = gps.TMS.build((layer_uri, nlcd_layer_name), nlcd_color_map)
nlcd_tms.bind("0.0.0.0", 54970)

nlcd_map = folium.Map()
folium.TileLayer(tiles='http://localhost:54970/tile/{z}/{x}/{y}.png',
                 attr="GeoPySpark").add_to(nlcd_map)

nlcd_map
```

### Reading and Formatting the NED Data


```python
ned_location = 's3://azavea-datahub/raw/ned-13arcsec-geotiff/'

# Reading in the NED data

ned = gps.geotiff.get(
    gps.LayerType.SPATIAL,
    ned_location,
    num_partitions=1000,
    max_tile_size=256)

# Tiling it to the osm_raster's layout

tiled_ned = ned.tile_to_layout(
    osm_raster.layer_metadata,
    partition_strategy=gps.SpatialPartitionStrategy(1000)
).convert_data_type(gps.CellType.FLOAT32, 0.0).persist(StorageLevel.MEMORY_AND_DISK_SER)
```

### Saving the NED Layer


```python
ned_wm = tiled_ned.tile_to_layout(gps.GlobalLayout(), target_crs=3857)
ned_pyramid = ned_wm.pyramid(partition_strategy=gps.SpatialPartitionStrategy(1000))

ned_layer_name = "ned-layer"

# Save layer histogram for later use

ned_hist = ned_pyramid.get_histogram()
store.layer(ned_layer_name).write("histogram", ned_hist.to_dict())

# Save layer pyramid

for zoom, layer in sorted(ned_pyramid.levels.items(), reverse=True):
    print("Writing zoom", zoom)
    store.layer(ned_layer_name, zoom).delete("metadata")
    gps.write(layer_uri, ned_layer_name, layer)
```

### Displaying the NED Data


```python
ned_layer_name = "ned-layer"
ned_hist = gps.Histogram.from_dict(store.layer(ned_layer_name).read("histogram"))
ned_color_map = gps.ColorMap.build(ned_hist, 'magma')

ned_tms = gps.TMS.build((layer_uri, ned_layer_name), ned_color_map)
ned_tms.bind("0.0.0.0", 59610)

ned_map = folium.Map()
folium.TileLayer(tiles='http://localhost:59610/tile/{z}/{x}/{y}.png',
                 attr="GeoPySpark").add_to(ned_map)

ned_map
```

## Caculating Tobler Walking Speeds


```python
# Calculate Slope from the NED layer

zfactor = gps.geotrellis.zfactor_lat_lng_calculator('Meters')
slope_raster = tiled_ned.slope(zfactor)

# From the Slope layer, calculate the Tobler walking speed

tobler_raster = slope_raster.tobler()

# Add the Tobler and Reclassified NLCD layers to adjusted the Tobler values

adjusted_tobler = tobler_raster + nlcd_pmts

# The Friction layer is produced by taking the local max between the adjusted Tobler values and the
# rasterized OSM layer

friction_with_roads = adjusted_tobler.local_max(osm_raster)

reprojected = friction_with_roads.tile_to_layout(
    target_crs = 3857,
    layout = gps.GlobalLayout(tile_size=256),
    resample_method = gps.ResampleMethod.MAX
).convert_data_type(gps.CellType.FLOAT32, 0.0)

pyramid = reprojected\
    .pyramid(partition_strategy=gps.SpatialPartitionStrategy(1000))\
    .persist()
```

## Writing the Friction Layer to S3


```python
layer_name = "us-friction-surface-layer-tms"

# Save layer histogram for later use

hist = pyramid.get_histogram()
store.layer(layer_name).write("histogram", hist.to_dict())

# Save layer pyramid

for zoom, layer in sorted(pyramid.levels.items(), reverse=True):
    print("Writing zoom", zoom)
    store.layer(layer_name, zoom).delete("metadata")
    gps.write(layer_uri, layer_name, layer)
```

## Displaying the Fricition Layer


```python
layer_name = "us-friction-surface-layer-tms"
hist = gps.Histogram.from_dict(store.layer(layer_name).read("histogram"))
color_map = gps.ColorMap.build(hist, 'magma')

tms = gps.TMS.build((layer_uri, layer_name), color_map)
tms.bind("0.0.0.0", 55110)

friction_map = folium.Map()
folium.TileLayer(tiles='http://localhost:55110/tile/{z}/{x}/{y}.png',
                 attr="GeoPySpark").add_to(friction_map)

friction_map
```

## Calculating Cost Distance From the Friction Layer


```python
# The point of interest

point = Point(-75.15415012836456, 39.96134940667086)


# The point of interest needs to be reprojected to WebMercator in order
# to perform cost distance

project = partial(
    pyproj.transform,
    pyproj.Proj(init='epsg:4326'),
    pyproj.Proj(init='epsg:3857'))

reprojected_point = transform(project, point)

# Calculate Cost Distance using the Quotient of the average walking speed divided by the reprojected
# friction layer

cost_distance = gps.cost_distance(3.74 / reprojected,
                                  [reprojected_point],
                                  max_distance=50000).persist(StorageLevel.MEMORY_AND_DISK_SER)
```

### Saving the Cost Distance Layer


```python
cost_pyramid = cost_distance.pyramid(partition_strategy=gps.SpatialPartitionStrategy(1000))

cost_layer_name = "cost-distance"

# Save layer histogram for later use

cost_hist = cost_pyramid.get_histogram()
store.layer(cost_layer_name).write("histogram", cost_hist.to_dict())

# Save layer pyramid

for zoom, layer in sorted(cost_pyramid.levels.items(), reverse=True):
    print("Writing zoom", zoom)
    store.layer(cost_layer_name, zoom).delete("metadata")
    gps.write(layer_uri, cost_layer_name, layer)
```

### Displaying the Cost Distance Layer


```python
cost_layer_name = "cost-distance-2"
cost_hist = gps.Histogram.from_dict(store.layer(cost_layer_name).read("histogram"))
cost_color_map = gps.ColorMap.build(cost_hist, 'magma')
cost_tms = gps.TMS.build((layer_uri, cost_layer_name), cost_color_map)
cost_tms.bind("0.0.0.0", 50208)

cost_map = folium.Map()
folium.TileLayer(tiles='http://localhost:50208/tile/{z}/{x}/{y}.png',
                 attr="GeoPySpark").add_to(cost_map)

cost_map
```
