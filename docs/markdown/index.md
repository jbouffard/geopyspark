# What is GeoPySpark?

*GeoPySpark* is a Python language binding library of the Scala library,
GeoTrellis. Like GeoTrellis, this project is released under the Apache 2 License.

GeoPySpark seeks to utilize GeoTrellis to allow for the reading, writing, and
operating on raster data. Thus, its able to scale to the data and still be able
to perform well.

In addition to raster processing, GeoPySpark allows for rasters to be rendered
into PNGs. One of the goals of this project to be able to process rasters at
web speeds and to perform batch processing of large data sets.

## Why GeoPySpark?

Raster processing in Python has come a long way; however, issues still arise
as the size of the dataset increases. Whether it is performance or ease of use,
these sorts of problems will become more common as larger amounts of data are
made available to the public.

One could turn to GeoTrellis to resolve the aforementioned problems (and one
should try it out!), yet this brings about new challenges. Scala, while a
powerful language, has something of a steep learning curve. This can put off
those who do not have the time and/or interest in learning a new language.

By having the speed and scalability of Scala and the ease of Python,
GeoPySpark is then the remedy to this predicament.

**Table of Contents:**
- User Guides
 - [Core Concepts](guides/core-concepts.md)
 - [Working With Layers](guides/layers.md)
 - [Map Algebra](guides/map-algebra.md)
 - [Partition Strategy](guides/partition-strategy.md)
 - [Visualizing Layers](guides/visualization.md)
 - [Catalogs](guides/catalog.md)
 - [TMS](guides.tms.md)
- Tutorials
 - [Reading and Rasterizing OSM Data](tutorials/reading-and-rasterizing-osm-data)
