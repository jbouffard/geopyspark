
# Calculating XTerrain for Djibouti


```python
# Importing our libraries

import geopyspark as gps
import numpy as np
import pyproj
import fiona
import folium

from functools import partial
from shapely.geometry import Point, box
from shapely.ops import transform
from pyspark import SparkContext, StorageLevel
from pyspark.sql import SparkSession
```


```python
# Setting up the SparkContext

# Most of these settings are required for running the notebook locally,
# but we've found that they work when running it on a cluster
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

# In order to read orc files from S3, we need to pass in our S3 credentials
# to the hadoopConf
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
layer_uri = "s3://gt-rasters/xterrain-example/catalog"

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
  - GlobCover: Land cover
  - Shuttle Radar Topography (SRTM): Elevation

### Reading and Formatting the OSM Data

**NOTE:** The `org.apache.hadoop:hadoop-aws:2.7.1` package must be in the Spark classpath in order to read `orc` files from s3.


```python
# Read in the OSM data as an ORC file
file_uri = "s3://gt-rasters/xterrain-example/djibouti.orc"
osm_dataframe = session.read.orc(file_uri)
```


```python
# Get all of the lines that are contained within the DataFrame
osm = gps.vector_pipe.osm_reader.from_dataframe(osm_dataframe)
lines = osm.get_line_features_rdd()
```

#### Roads and Trails


```python
highways = lines.filter(lambda feature: 'highway' in feature.properties.tags)
```


```python
path_tags = ['footway', 'steps', 'bridleway', 'path', 'cycleway', 'escalator']

# Filter out the highways into roads and paths

roads = highways.filter(lambda feature: feature.properties.tags['highway'] not in path_tags)
paths = highways.filter(lambda feature: feature.properties.tags['highway'] in path_tags)
```


```python
# Encode the the paths with the default walking speed
path_features = paths\
    .map(lambda feature: gps.Feature(feature.geometry, gps.CellValue(3.74, 3.74)))\
    .persist(StorageLevel.MEMORY_AND_DISK_SER)
```


```python
# This cell contains the logic that assigns each section of road a
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
```


```python
# Encode the road speeds as feature properties for rasterization
road_features = roads\
    .map(lambda feature: gps.Feature(feature.geometry, get_highway_cellvalue(feature)))\
    .persist(StorageLevel.MEMORY_AND_DISK_SER)
```

#### Waterways


```python
waterways = lines.filter(lambda feature: 'waterway' in feature.properties.tags)
```


```python
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
```


```python
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
    partition_strategy = gps.SpatialPartitionStrategy(8))\
.convert_data_type(gps.CellType.FLOAT32, -2147483648.0)\
.persist(StorageLevel.MEMORY_AND_DISK_SER)
```

### Saving the Rasterized OSM Features


```python
tiled_osm = osm_raster.tile_to_layout(gps.GlobalLayout(), target_crs=3857).with_no_data(0.0)
osm_pyramid = tiled_osm.pyramid(partition_strategy=gps.SpatialPartitionStrategy(1000))
```


```python
osm_layer_name = "rasterized-osm-features"
```


```python
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
```


```python
# NOTE: In order to display the maps when working on a cluster,
# the tiles parameter needs to be the Master Public DNS of the
# cluster followed by the port number

osm_map = folium.Map()
folium.TileLayer(tiles='http://localhost:56583/tile/{z}/{x}/{y}.png',
                 attr="GeoPySpark").add_to(osm_map)
```


```python
osm_map
```

### Reading and Formatting the Glob Cover Data


```python
# Reading Glob Cover Data
# WARNING: This file seems to be too large to read and format locally
glob_cover = gps.geotiff.get(
    gps.LayerType.SPATIAL, 
    "s3://gt-rasters/xterrain-example/GLOBCOVER_L4_200901_200912_V2.3.color.tif",
    max_tile_size=128,
    num_partitions=8)
```


```python
tiled_glob_cover = glob_cover.tile_to_layout(
    osm_raster.layer_metadata,
    target_crs=4326, 
    partition_strategy=gps.SpatialPartitionStrategy(8))
```


```python
djibouti_area = box(41.165771484375, 10.628216112538693, 44.40673828125, 10.628216112538693)

masked_glob_cover = tiled_glob_cover.mask(geometries=djibouti_area, partition_strategy=gps.SpatialPartitionStrategy(8))
```


```python
# Reclassify the GlobCover values based on estimated walking impact

glob_cover_map = {
    11.0: 0.2,
    14.0: 0.8,
    20.0: 0.8,
    30.0: 0.7,
    40.0: 0.7,
    50.0: 0.7,
    60.0: 0.7,
    70.0: 0.7,
    80.0: 0.7,
    90.0: 0.7,
    100.0: 0.7,
    110.0: 0.75,
    120.0: 0.8,
    130.0: 0.8,
    140.0: 0.8,
    150.0: 0.85,
    160.0: 0.4,
    170.0: 0.2,
    180.0: 0.25,
    190.0: 0.9,
    210.0: 0.0,
    200.0: 0.1,
    220.0: 0.15
}

glob_cover_pmts = masked_glob_cover\
    .convert_data_type(gps.CellType.FLOAT32, 0.0)\
    .reclassify(glob_cover_map, float, gps.ClassificationStrategy.EXACT)
```

### Saving the Reclassified Glob Cover Layer


```python
glob_cover_wm = glob_cover_pmts.tile_to_layout(gps.GlobalLayout(), target_crs=3857).persist(StorageLevel.MEMORY_AND_DISK_SER)
glob_cover_pyramid = glob_cover_wm.pyramid(partition_strategy=gps.SpatialPartitionStrategy(8))
```


```python
glob_cover_layer_name = "raclassified-glob-cover"
```


```python
# Save layer histogram for later use
glob_cover_hist = glob_cover_pyramid.get_histogram()
store.layer(glob_cover_layer_name).write("histogram", glob_cover_hist.to_dict())

# Save layer pyramid
for zoom, layer in sorted(glob_cover_pyramid.levels.items(), reverse=True):
    print("Writing zoom", zoom)
    store.layer(glob_cover_layer_name, zoom).delete("metadata")
    gps.write(layer_uri, glob_cover_layer_name, layer)
```

### Displaying the Reclassified Glob Cover Data


```python
glob_cover_layer_name = "raclassified-glob-cover"
glob_cover_hist = gps.Histogram.from_dict(store.layer(glob_cover_layer_name).read("histogram"))
glob_cover_map = gps.ColorMap.build(glob_cover_hist, 'magma')

glob_cover_tms = gps.TMS.build((layer_uri, glob_cover_layer_name), glob_cover_color_map)
glob_cover_tms.bind("0.0.0.0", 54970)
```


```python
# NOTE: In order to display the maps when working on a cluster,
# the tiles parameter needs to be the Master Public DNS of the
# cluster followed by the port number

glob_cover_map = folium.Map()
folium.TileLayer(tiles='http://localhost:54970/tile/{z}/{x}/{y}.png',
                 attr="GeoPySpark").add_to(glob_cover_map)
```


```python
glob_cover_map
```

### Reading and Formatting the SRTM Data


```python
srtm_location = 's3://gt-rasters/xterrain-example/srtm_45_10.tif'
```


```python
srtm = gps.geotiff.get(
    gps.LayerType.SPATIAL, 
    srtm_location, 
    num_partitions=8, 
    max_tile_size=256)
```


```python
tiled_srtm = srtm.tile_to_layout(
    osm_raster.layer_metadata, 
    partition_strategy=gps.SpatialPartitionStrategy(8)
).convert_data_type(gps.CellType.FLOAT32, 0.0).persist(StorageLevel.MEMORY_AND_DISK_SER)
```

### Saving the SRTM Layer


```python
srtm_wm = tiled_srtm.tile_to_layout(gps.GlobalLayout(), target_crs=3857)
srtm_pyramid = srtm_wm.pyramid(partition_strategy=gps.SpatialPartitionStrategy(8))
```


```python
srtm_layer_name = "srtm-layer"
```


```python
# Save layer histogram for later use
srtm_hist = srtm_pyramid.get_histogram()
store.layer(srtm_layer_name).write("histogram", srtm_hist.to_dict())

# Save layer pyramid
for zoom, layer in sorted(srtm_pyramid.levels.items(), reverse=True):
    print("Writing zoom", zoom)
    store.layer(srtm_layer_name, zoom).delete("metadata")
    gps.write(layer_uri, srtm_layer_name, layer)
```

### Displaying the SRTM Data


```python
srtm_layer_name = "srtm-layer"
srtm_hist = gps.Histogram.from_dict(store.layer(srtm_layer_name).read("histogram"))
srtm_color_map = gps.ColorMap.build(srtm_hist, 'magma')

srtm_tms = gps.TMS.build((layer_uri, srtm_layer_name), srtm_color_map)
srtm_tms.bind("0.0.0.0", 59610)
```


```python
# NOTE: In order to display the maps when working on a cluster,
# the tiles parameter needs to be the Master Public DNS of the
# cluster followed by the port number

srtm_map = folium.Map()
folium.TileLayer(tiles='http://localhost:59610/tile/{z}/{x}/{y}.png',
                 attr="GeoPySpark").add_to(srtm_map)
```


```python
srtm_map
```

## Caculating Tobler Walking Speeds


```python
# Calculate Slope from the NED layer
zfactor = gps.geotrellis.zfactor_lat_lng_calculator('Meters')
slope_raster = tiled_srtm.slope(zfactor)
```


```python
# From the Slope layer, calculate the Tobler walking speed
tobler_raster = slope_raster.tobler()

# Add the Tobler and Reclassified Glob Cover layers to adjusted the Tobler values
adjusted_tobler = tobler_raster# + glob_cover_pmts
```


```python
# The Friction layer is produced by taking the local max between the adjusted Tobler values and the
# rasterized OSM layer
friction_with_roads = adjusted_tobler.local_max(osm_raster)
```


```python
reprojected = friction_with_roads.tile_to_layout(
    target_crs = 3857,
    layout = gps.GlobalLayout(tile_size=256),
    resample_method = gps.ResampleMethod.MAX
).convert_data_type(gps.CellType.FLOAT32, 0.0)
```


```python
pyramid = reprojected\
    .pyramid(partition_strategy=gps.SpatialPartitionStrategy(8))\
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
```


```python
# NOTE: In order to display the maps when working on a cluster,
# the tiles parameter needs to be the Master Public DNS of the
# cluster followed by the port number

friction_map = folium.Map()
folium.TileLayer(tiles='http://localhost:55110/tile/{z}/{x}/{y}.png',
                 attr="GeoPySpark").add_to(friction_map)
```


```python
friction_map
```

## Calculating Cost Distance From the Friction Layer


```python
# The point of interest
point = Point(43.14356803894043, 11.580774662762202)
```


```python
# The point of interest needs to be reprojected to WebMercator in order
# to perform cost distance

project = partial(
    pyproj.transform,  
    pyproj.Proj(init='epsg:4326'),
    pyproj.Proj(init='epsg:3857'))

reprojected_point = transform(project, point)
```


```python
# Calculate Cost Distance using the Quotient of the average walking speed divided by the reprojected
# friction layer
cost_distance = gps.cost_distance(3.74 / reprojected,
                                  [reprojected_point],
                                  max_distance=50000).persist(StorageLevel.MEMORY_AND_DISK_SER)
```

### Saving the Cost Distance Layer


```python
cost_pyramid = cost_distance.pyramid(partition_strategy=gps.SpatialPartitionStrategy(8))

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
cost_layer_name = "cost-distance"
cost_hist = gps.Histogram.from_dict(store.layer(cost_layer_name).read("histogram"))
cost_color_map = gps.ColorMap.build(cost_hist, 'magma')
cost_tms = gps.TMS.build((layer_uri, cost_layer_name), cost_color_map)
cost_tms.bind("0.0.0.0", 50208)
```


```python
# NOTE: In order to display the maps when working on a cluster,
# the tiles parameter needs to be the Master Public DNS of the
# cluster followed by the port number

cost_map = folium.Map()
folium.TileLayer(tiles='http://localhost:50208/tile/{z}/{x}/{y}.png',
                 attr="GeoPySpark").add_to(cost_map)
```


```python
cost_map
```
