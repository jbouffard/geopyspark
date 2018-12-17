# Partition Strategies

When working with distributed spatial data, one should
take into consideration how the data within the cluster
is partitioned. As values that are spatially near each other
are often involved the same operation. Therefore, if the
data to perform the operation is spread out across the cluster,
shuffiling will occur.

To help avoid this, there are different partitioning
strategies that can be used to ensure that spatially
close data are grouped together.

## HashPartitionStrategy

`HashPartitionStrategy` is a partition strategy that uses Spark's
`HashPartitioner` to partition a layer. This can be used on either `SPATIAL`
or `SPACETIME` layers.

Unlike `SpatialPartitionStrategy` and `SpaceTimePartitionStrategy`,
`HashPartitionerStrategy` does not partition data spatially.

```python3

# Creates a HashPartitionStrategy with 128 partitions
gps.HashPartitionStrategy(num_partitions=128)
```

## Spatial Partitioning Strategies

There are several benefits to using a spatial partitioning strategy over the
`HashPartitioner`:

- Scales the number of partitions with the number of elements in the layer.
- Faster focal operations
- Suffle free joins with other layers that use the same partitioner
- Efficient spatial region filtering

### SpatialPartitionStrategy

`SpatialPartitionStrategy` will try and partition the `Tile`s of a layer so
that those which are near each other spatially will be in the same partition.
This will only work on `SPATIAL` layers.

```python3

# Creates a SpatialPartitionStrategy with 128 partitions
gps.SpatialPartitionStrategy(num_partitions=128)
```

### SpaceTimePartitionStrategy

`SpaceTimePartitionStrategy` will try and partition the `Tile`s of a layer
so that those which are near each other spatially and temporally will be in the
same partition.  This will only work on `SPACETIME` layers.

```python3

# Creates a SpaceTimePartitionStrategy with 128 partitions
# and temporal resolution of 5 weeks. This means that
# it will try and group the data in units of 5 weeks.
gps.SpaceTimePartitionStrategy(time_unit=gps.WEEKS, num_partitions=128, time_resolution=5)
```
