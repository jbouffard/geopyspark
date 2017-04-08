package geopyspark.geotrellis.io.geotiff

import geopyspark.geotrellis._

import geotrellis.proj4._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.s3._
import geotrellis.spark.io.s3.testkit._

import scala.collection.JavaConverters._

import java.net.URI
import java.util.Map

import org.apache.spark._
import org.apache.hadoop.fs.Path


object GeoTiffRDD {
  import Constants._

  object HadoopGeoTiffRDDOptions {
    def default = HadoopGeoTiffRDD.Options.DEFAULT

    def setValues(javaMap: java.util.Map[String, Any]): HadoopGeoTiffRDD.Options = {
      val stringValues = Array("timeTag", "timeFormat", "crs")

      val (stringMap, intMap) = GeoTrellisUtils.convertToScalaMap(javaMap, stringValues)

      val crs: Option[CRS] =
        if (stringMap.contains("crs"))
          Some(CRS.fromName(stringMap("crs")))
        else
          None

      HadoopGeoTiffRDD.Options(
        crs = crs,
        timeTag = stringMap.getOrElse("timeTag", default.timeTag),
        timeFormat = stringMap.getOrElse("timeFormat", default.timeFormat),
        maxTileSize = intMap.get("maxTileSize"),
        numPartitions = intMap.get("numPartitions"),
        chunkSize = intMap.get("chunkSize"))
    }
  }

  object S3GeoTiffRDDOptions {
    def default = S3GeoTiffRDD.Options.DEFAULT

    def setValues(javaMap: java.util.Map[String, Any]): S3GeoTiffRDD.Options = {
      val stringValues = Array("timeTag", "timeFormat", "s3Client", "crs")
      val (stringMap, intMap) = GeoTrellisUtils.convertToScalaMap(javaMap, stringValues)

      val crs: Option[CRS] =
        if (stringMap.contains("crs"))
          Some(CRS.fromName(stringMap("crs")))
        else
          None

      val getS3Client: () => S3Client =
        stringMap.get("s3Client") match {
          case Some(client) =>
            if (client == "default")
              default.getS3Client
            else if (client == "mock")
              () => new MockS3Client()
            else
              throw new Error(s"Could not find the given S3Client, $client")
          case None => default.getS3Client
        }

      S3GeoTiffRDD.Options(
        crs = crs,
        timeTag = stringMap.getOrElse("timeTag", default.timeTag),
        timeFormat = stringMap.getOrElse("timeFormat", default.timeFormat),
        maxTileSize = intMap.get("maxTileSize"),
        numPartitions = intMap.get("numPartitions"),
        chunkSize = intMap.get("chunkSize"),
        getS3Client = getS3Client)
    }
  }

  def get(
    sc: SparkContext,
    keyType: String,
    path: String
  ): RasterRDD[_] = {
    val uri = new URI(path)

    uri.getScheme match {
      case S3 =>
        getS3GeoTiffRDD(sc, keyType, uri, S3GeoTiffRDDOptions.default)
      case _ =>
        getHadoopGeoTiffRDD(sc, keyType, new Path(path), HadoopGeoTiffRDDOptions.default)
    }
  }

  def get(
    sc: SparkContext,
    keyType: String,
    path: String,
    options: java.util.Map[String, Any]
  ): RasterRDD[_] = {
    val uri = new URI(path)

    uri.getScheme match {
      case S3 =>
        getS3GeoTiffRDD(sc, keyType, uri, S3GeoTiffRDDOptions.setValues(options))
      case _ =>
        getHadoopGeoTiffRDD(sc, keyType, new Path(path), HadoopGeoTiffRDDOptions.setValues(options))
    }
  }

  private def getHadoopGeoTiffRDD(
    sc: SparkContext,
    keyType: String,
    path: Path,
    options: HadoopGeoTiffRDD.Options
  ): RasterRDD[_] =
    keyType match {
      case PROJECTEDEXTENT =>
        ProjectedRasterRDD(HadoopGeoTiffRDD.spatialMultiband(path, options)(sc))
      case TEMPORALPROJECTEDEXTENT =>
        TemporalRasterRDD(HadoopGeoTiffRDD.temporalMultiband(path, options)(sc))
    }

  private def getS3GeoTiffRDD(
    sc: SparkContext,
    keyType: String,
    uri: URI,
    options: S3GeoTiffRDD.Options
  ): RasterRDD[_] =
    keyType match {
      case PROJECTEDEXTENT =>
        ProjectedRasterRDD(S3GeoTiffRDD.spatialMultiband(uri.getHost, uri.getPath.tail, options)(sc))
      case TEMPORALPROJECTEDEXTENT =>
        TemporalRasterRDD(S3GeoTiffRDD.temporalMultiband(uri.getHost, uri.getPath.tail, options)(sc))
    }
}