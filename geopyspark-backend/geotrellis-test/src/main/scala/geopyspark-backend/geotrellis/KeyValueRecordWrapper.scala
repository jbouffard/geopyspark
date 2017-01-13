package geopyspark.geotrellis.testkit

import geotrellis.raster._
import geotrellis.vector.Extent
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._
import geotrellis.spark.util.KryoWrapper
import geotrellis.spark.io.avro.codecs._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD
import org.apache.avro._

object TileExtentKeyValueRecordWrapper extends KeyValueWrapper[ByteArrayTile, Extent] {

  def codec: KeyValueRecordCodec[ByteArrayTile, Extent] = KeyValueRecordCodec[ByteArrayTile, Extent]

  def testRdd(sc: SparkContext): RDD[Vector[(ByteArrayTile, Extent)]] = {
    val vector = Vector(
      (ByteArrayTile(Array[Byte](0, 1, 2, 3, 4, 5), 2, 3) -> Extent(0, 0, 1, 1)),
      (ByteArrayTile(Array[Byte](0, 1, 2, 3, 4, 5), 3, 2) -> Extent(1, 2, 3, 4)),
      (ByteArrayTile(Array[Byte](0, 1, 2, 3, 4, 5), 1, 6) -> Extent(5, 6, 7, 8)))

    val arr = Array(vector, vector)

    sc.parallelize(arr)
  }
}
