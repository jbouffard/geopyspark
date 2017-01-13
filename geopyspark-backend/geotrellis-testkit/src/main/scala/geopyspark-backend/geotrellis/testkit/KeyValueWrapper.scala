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

abstract class KeyValueWrapper[K: AvroRecordCodec, V: AvroRecordCodec] {

  def codec: KeyValueRecordCodec[K, V]

  def testOut(sc: SparkContext) =
    toPython(testRdd(sc))

  def testIn(rdd: RDD[Array[Byte]], schema: String) =
    fromPython(rdd, Some(schema)).foreach(println)

  def encodeRdd(rdd: RDD[Vector[(K, V)]]): RDD[Array[Byte]] = {
    rdd.map { key => AvroEncoder.toBinary(key, deflate = false)(codec)
    }
  }

  def encodeRddText(rdd: RDD[Vector[(K, V)]]): RDD[String] = {
      rdd.map { key => AvroEncoder.toBinary(key, deflate = false)(codec).mkString("")
      }
    }

  def keySchema: String = {
    codec.schema.toString
  }

  def testRdd(sc: SparkContext): RDD[Vector[(K, V)]]

  def toPython(rdd: RDD[Vector[(K, V)]]): (JavaRDD[Array[Byte]], String) = {
    val jrdd =
      rdd
        .map { v =>
          AvroEncoder.toBinary(v, deflate = false)(codec)
        }
        .toJavaRDD
    (jrdd, keySchema)
  }

  def fromPython(rdd: RDD[Array[Byte]], schemaJson: Option[String] = None): RDD[Vector[(K, V)]] = {
    val schema = schemaJson.map { json => (new Schema.Parser).parse(json) }
    val _recordCodec = codec
    val kwWriterSchema = KryoWrapper(schema)

    rdd.map { bytes =>
      AvroEncoder.fromBinary(kwWriterSchema.value.getOrElse(_recordCodec.schema), bytes, false)(_recordCodec)
    }
  }
}
