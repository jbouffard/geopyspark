package geopyspark.geotrellis

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{GeoTiffOptions, MultibandGeoTiff, Tags}
import geotrellis.raster.resample.ResampleMethod
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.tiling._
import geotrellis.vector._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd._
import protos.tupleMessages.ProtoTuple

import scala.util.{Either, Left, Right}
import spray.json._


class ProjectedRasterRDD(val rdd: RDD[(ProjectedExtent, MultibandTile)]) extends RasterRDD[ProjectedExtent] {

  def collectMetadata(layout: Either[LayoutScheme, LayoutDefinition], crs: Option[CRS]): String = {
    (crs, layout) match {
      case (Some(crs), Right(layoutDefinition)) =>
        rdd.collectMetadata[SpatialKey](crs, layoutDefinition)
      case (None, Right(layoutDefinition)) =>
        rdd.collectMetadata[SpatialKey](layoutDefinition)
      case (Some(crs), Left(layoutScheme)) =>
        rdd.collectMetadata[SpatialKey](crs, layoutScheme)._2
      case (None, Left(layoutScheme)) =>
        rdd.collectMetadata[SpatialKey](layoutScheme)._2
    }
  }.toJson.compactPrint

  def cutTiles(layerMetadata: String, resampleMethod: String): TiledRasterRDD[SpatialKey] = {
    val md = layerMetadata.parseJson.convertTo[TileLayerMetadata[SpatialKey]]
    val rm = TileRDD.getResampleMethod(resampleMethod)
    new SpatialTiledRasterRDD(None, MultibandTileLayerRDD(rdd.cutTiles(md, rm), md))
  }

  def tileToLayout(tileLayerMetadata: String, resampleMethod: String): TiledRasterRDD[SpatialKey] = {
    val md = tileLayerMetadata.parseJson.convertTo[TileLayerMetadata[SpatialKey]]
    val rm = TileRDD.getResampleMethod(resampleMethod)
    new SpatialTiledRasterRDD(None, MultibandTileLayerRDD(rdd.tileToLayout(md, rm), md))
  }

  def tileToLayout(
    layoutType: LayoutType,
    rm: ResampleMethod,
    force_crs: String,
    force_cellType: String
  ): TiledRasterRDD[SpatialKey] = {
    val sms = collectRasterSummary[SpatialKey]
    require(sms.length == 1, s"Multiple raster CRS layers found: ${sms.map(_.crs).toList}")
    val sm = sms.head
    val md = sm.toTileLayerMetadata(layoutType)
    val tiled = rdd.tileToLayout(md, rm)
    new SpatialTiledRasterRDD(None, MultibandTileLayerRDD(tiled, md))
  }

  def reproject(targetCRS: String, resampleMethod: String): ProjectedRasterRDD = {
    val crs = TileRDD.getCRS(targetCRS).get
    val resample = TileRDD.getResampleMethod(resampleMethod)
    new ProjectedRasterRDD(rdd.reproject(crs, resample))
  }

  def reclassify(reclassifiedRDD: RDD[(ProjectedExtent, MultibandTile)]): RasterRDD[ProjectedExtent] =
    ProjectedRasterRDD(reclassifiedRDD)

  def reclassifyDouble(reclassifiedRDD: RDD[(ProjectedExtent, MultibandTile)]): RasterRDD[ProjectedExtent] =
    ProjectedRasterRDD(reclassifiedRDD)

  def withRDD(result: RDD[(ProjectedExtent, MultibandTile)]): RasterRDD[ProjectedExtent] =
    ProjectedRasterRDD(result)

  def toProtoRDD(): JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[(ProjectedExtent, MultibandTile), ProtoTuple](rdd)

  def toPngRDD(pngRDD: RDD[(ProjectedExtent, Array[Byte])]): JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[(ProjectedExtent, Array[Byte]), ProtoTuple](pngRDD)

  def toGeoTiffRDD(
    tags: Tags,
    geotiffOptions: GeoTiffOptions
  ): JavaRDD[Array[Byte]] = {
    val geotiffRDD = rdd.map { x =>
      (x._1, MultibandGeoTiff(x._2, x._1.extent, x._1.crs, tags, geotiffOptions).toByteArray)
    }

    PythonTranslator.toPython[(ProjectedExtent, Array[Byte]), ProtoTuple](geotiffRDD)
  }
}

object ProjectedRasterRDD {
  def fromProtoEncodedRDD(javaRDD: JavaRDD[Array[Byte]]): ProjectedRasterRDD =
    ProjectedRasterRDD(
      PythonTranslator.fromPython[
        (ProjectedExtent, MultibandTile), ProtoTuple
      ](javaRDD, ProtoTuple.parseFrom))

  def apply(rdd: RDD[(ProjectedExtent, MultibandTile)]): ProjectedRasterRDD =
    new ProjectedRasterRDD(rdd)
}