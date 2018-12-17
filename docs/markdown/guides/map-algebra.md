# Map Algebra

Given a set of layers, it may be desirable to combine and filter
their contents of those layers. This is the function of *map algebra*. Two
classes of map algebra operations are provided by GeoPySpark: *local*
and *focal* operations. Local operations individually consider the
pixels or cells of one or more rasters, and applying a function to the
corresponding cell values. For example, adding two rasters' pixel values
to form a new layer is a local operation.

Focal operations consider a region around each pixel of an input raster,
and they apply an operation to the region. The result of that operation is
stored in the corresponding pixel of the output raster. For example, one
might weight a 5x5 region centered at a pixel according to a 2d Gaussian
to effect a blurring of the input raster. This is roughly equivalent to
a 2d convolution operation.

**Note:** Map algebra operations work only on `TiledRasterLayer`s
and `Pyramid`s; and if a local operation requires multiple inputs,
those inputs must have the same layout and projection.

Before begining, all examples in this guide need the following boilerplate
code:

```python3

   import geopyspark as gps
   import numpy as np

   from pyspark import SparkContext
   from shapely.geometry import Point, MultiPolygon, LineString, box

   conf = gps.geopyspark_conf(master="local[*]", appName="map-algebra")
   pysc = SparkContext(conf=conf)

   # Setting up the data

   cells = np.array([[[3, 4, 1, 1, 1],
                      [7, 4, 0, 1, 0],
                      [3, 3, 7, 7, 1],
                      [0, 7, 2, 0, 0],
                      [6, 6, 6, 5, 5]]], dtype='int32')

   extent = gps.ProjectedExtent(extent = gps.Extent(0, 0, 5, 5), epsg=4326)

   layer = [(extent, gps.Tile.from_numpy_array(numpy_array=cells))]

   rdd = pysc.parallelize(layer)
   raster_layer = gps.RasterLayer.from_numpy_rdd(gps.LayerType.SPATIAL, rdd)
   tiled_layer = raster_layer.tile_to_layout(layout=gps.LocalLayout(tile_size=5))
```

## Local Operations

Local operations on `TiledRasterLayer`s can use `int`s,
`float`s, or other `TiledRasterLayer`s. `+`, `-`, `*`,
`/`, `**`, and `abs` are all of the local operations that currently supported.

```python3

    (tiled_layer + 1)

    (2 - (tiled_layer * 3))

    ((tiled_layer + tiled_layer) / (tiled_layer + 1))

    abs(tiled_layer)

    2 ** tiled_layer
```

`Pyramid`s can also be used in local operations. The types that
can be used in local operations with `Pyramid`s are: `int`s,
`float`s, `TiledRasterLayer`s, and other `Pyramid`s.

**Note**: Like with `TiledRasterLayer`, performing calculations on
multiple `Pyramid`s or `TiledRasterLayer`s means they must all
have the same layout and projection.

```python3

    # Creating out Pyramid
    pyramid = tiled_layer.pyramid()

    pyramid + 1

    (pyramid - tiled_layer) * 2
```

## Focal Operations

Focal operations are performed in GeoPySpark by executing a given
operation on a neighborhood throughout each tile in the layer. One can
select a neighborhood to use from the `Neighborhood` enum class.
Likewise, an operation can be choosen from the enum class,
`Operation`.

```python3

    # This creates an instance of Square with an extent of 1. This means that
    # each operation will be performed on a 3x3
    # neighborhood.

    '''
    A square neighborhood with an extent of 1.
    o = source cell
    x = cells that fall within the neighbhorhood

    x x x
    x o x
    x x x
    '''

    square = gps.Square(extent=1)
```

### Mean

```python3

    tiled_layer.focal(operation=gps.Operation.MEAN, neighborhood=square)
```

### Median

```python3

    tiled_layer.focal(operation=gps.Operation.MEDIAN, neighborhood=square)
```

### Mode

```python3

    tiled_layer.focal(operation=gps.Operation.MODE, neighborhood=square)
```

### Sum

```python3

    tiled_layer.focal(operation=gps.Operation.SUM, neighborhood=square)
```

### Standard Deviation

```python3

    tiled_layer.focal(operation=gps.Operation.STANDARD_DEVIATION, neighborhood=square)
```

### Min

```python3

    tiled_layer.focal(operation=gps.Operation.MIN, neighborhood=square)
```

### Max

```python3

    tiled_layer.focal(operation=gps.Operation.MAX, neighborhood=square)
```

### Slope

```python3

    tiled_layer.focal(operation=gps.Operation.SLOPE, neighborhood=square)
```

### Aspect

```python3

    tiled_layer.focal(operation=gps.Operation.ASPECT, neighborhood=square)
```

## Polygonal Summary Methods

In addition to local and focal operations, polygonal summaries can also
be performed on `TiledRasterLayer`s. These are operations that are
executed in the areas that intersect a given geometry and the layer.

**Note**: It is important that the given geometry is in the same projection
as the layer. If they are not, then either incorrect and/or partial
results will be returned.

### Polygonal Min

```python3

    poly_min = box(0.0, 0.0, 1.0, 1.0)
    tiled_layer.polygonal_min(geometry=poly_min, data_type=int)
```

### Polygonal Max

```python3

    poly_max = box(1.0, 0.0, 2.0, 2.5)
    tiled_layer.polygonal_min(geometry=poly_max, data_type=int)
```

### Polygonal Sum

```python3

    poly_sum = box(0.0, 0.0, 1.0, 1.0)
    tiled_layer.polygonal_min(geometry=poly_sum, data_type=int)
```

### Polygonal Mean

```python3

    poly_max = box(1.0, 0.0, 2.0, 2.0)
    tiled_layer.polygonal_min(geometry=poly_max, data_type=int)
```

## Cost Distance

The `cost_distance` function is an iterative operation for approximating
the weighted distance from a raster cell to a given geometry. This function
takes in a geometry and a “friction layer” which essentially describes how
difficult it is to traverse each raster cell.  Cells that fall within the
geometry have a final cost of zero, while friction cells that contain noData
values will correspond to noData values in the final result. All other cells
have a value that describes the minimum cost of traversing from that cell to
the geometry. If the friction layer is uniform, this function approximates the
Euclidean distance, for some modulo scalar value.

```python3

    cost_distance_cells = np.array([[[1.0, 1.0, 1.0, 1.0, 1.0],
                                     [1.0, 1.0, 1.0, 1.0, 1.0],
                                     [1.0, 1.0, 1.0, 1.0, 1.0],
                                     [1.0, 1.0, 1.0, 1.0, 1.0],
                                     [1.0, 1.0, 1.0, 1.0, 0.0]]])

    tile = gps.Tile.from_numpy_array(numpy_array=cost_distance_cells, no_data_value=-1.0)
    cost_distance_extent = gps.ProjectedExtent(extent=gps.Extent(xmin=0.0, ymin=0.0, xmax=5.0, ymax=5.0), epsg=4326)
    cost_distance_layer = [(cost_distance_extent, tile)]

    cost_distance_rdd = pysc.parallelize(cost_distance_layer)
    cost_distance_raster_layer = gps.RasterLayer.from_numpy_rdd(gps.LayerType.SPATIAL, cost_distance_rdd)
    cost_distance_tiled_layer = cost_distance_raster_layer.tile_to_layout(layout=gps.LocalLayout(tile_size=5))

    gps.cost_distance(friction_layer=cost_distance_tiled_layer, geometries=[Point(0.0, 5.0)], max_distance=144000.0)
```

## Rasterization

It may be desirable to convert vector data into a raster layer. For
this, we provide two different rasterization functions: `rasterize`
and `rasterize_features`. Both of these functions will take a
series of vectors and assign the pixels they cover some value. However, they
differ in how they determine which value to assign the cell.

### rasterize

The `rasterize` function can take a `[shapely.geometry]`,
`(shapely.geometry)`, or a `PythonRDD[shapely.geometry]`. Given
these geometries and a `fill_value`, the vectors will be
converted to rasters whose cells' values will be the `fil_value`,
tiled to a given layout, and then be returned as a `TiledRasterLayer` which
contains these tiled values.

#### Rasterize MultiPolygons

```python3

    raster_poly_1 = box(0.0, 0.0, 5.0, 10.0)
    raster_poly_2 = box(3.0, 6.0, 15.0, 20.0)
    raster_poly_3 = box(13.5, 17.0, 30.0, 20.0)

    raster_multi_poly = MultiPolygon([raster_poly_1, raster_poly_2, raster_poly_3])

    # Creates a TiledRasterLayer with a CRS of EPSG:4326 at zoom level 5.
    gps.rasterize(geoms=[raster_multi_poly], crs=4326, zoom=5, fill_value=1)
```

#### Rasterize a PythonRDD of Polygons

```python3

    poly_rdd = pysc.parallelize([raster_poly_1, raster_poly_2, raster_poly_3])

    # Creates a TiledRasterLayer with a CRS of EPSG:3857 at zoom level 5.
    gps.rasterize(geoms=poly_rdd, crs=3857, zoom=3, fill_value=10)
```

#### Rasterize LineStrings

```python3

    line_1 = LineString(((0.0, 0.0), (0.0, 5.0)))
    line_2 = LineString(((7.0, 5.0), (9.0, 12.0), (12.5, 15.0)))
    line_3 = LineString(((12.0, 13.0), (14.5, 20.0)))

    # Creates a TiledRasterLayer whose cells have a data type of int16.
    gps.rasterize(geoms=[line_1, line_2, line_3], crs=4326, zoom=3, fill_value=2, cell_type=gps.CellType.INT16)
```

#### Rasterize Polygons and LineStrings

```python3

    # Creates a TiledRasterLayer from both LineStrings and MultiPolygons
    gps.rasterize(geoms=[line_1, line_2, line_3, raster_multi_poly], crs=4326, zoom=5, fill_value=2)
```

### rasterize_features

`rasterize_features` is similar to `rasterize` except that each given geometry
will have its own cell value. To accomplish this, we'll need to pass in our
vectors with additional information. This geometry + metadata is often called
a, `Feature`.

#### CellValue

Before rasterizing our features, we must consider two things: what cell value
each geometry is going to have and its priority. "priority" here means which
value to use if more than one geometry intersects a given cell.

The `CellValue` class holds both the cells'
`value` and its priority via the `zindex`. A `CellValue` with a
higher `zindex` will always be chosen over other `CellValue`s with lower
`zindex`es.

```python3

    # cell_value_3 will be chosen over cell_value_1 and cell_value_2 if all three
    # intersects the same cell. Likewise, cell_value_2 will be used instead of
    # cell_value_1 if those occupy the same cell(s)

    cell_value_1 = gps.CellValue(value=1, zindex=1)
    cell_value_2 = gps.CellValue(value=2, zindex=2)
    cell_value_3 = gps.CellValue(value=3, zindex=3)
```

#### Features

A `Feature` is an object that represents both a geometry and some associated
metadata. In the case of `rasterize_features`, this accompaning data is
`CellValue`.

Now that we have created our metadata, it is time to pair them with
the geometries so that we can create our `Feature`s.

```python3

    feature_1 = gps.Feature(geometry=line_1, properties=cell_value_1)
    feature_2 = gps.Feature(geometry=line_2, properties=cell_value_2)
    feature_3 = gps.Feature(geometry=line_3, properties=cell_value_3)
```


Now that we have our features created, we can now rasterize them.
As of right now, the `rasterize_features` function only takes a
`PythonRDD[gps.Feature]`.

```python3

    features_rdd = pysc.parallelize([feature_1, feature_2, feature_3])

    # Creates a TiledRasterLayer with a CRS of LatLng at zoom level 3
    gps.rasterize_features(features=features_rdd, crs=4326, zoom=3)
```
