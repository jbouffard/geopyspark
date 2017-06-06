package geopyspark.geotrellis.protobufs

import geopyspark.geotrellis._
import geopyspark.geotrellis.protobufs.Implicits._
import geotrellis.spark._

import protos.extentMessages.{TemporalProjectedExtent => ProtoTemporalProjectedExtent}


trait TemporalProjectedExtentProtoBuf {
/*
  implicit def temporalProjectedExtentProtoBufFormat = new ProtoBufCodec[TemporalProjectedExtent, ProtoTemporalProjectedExtent] {
    def encode(extent: TemporalProjectedExtent): ProtoTemporalProjectedExtent =
      ProtoTemporalProjectedExtent(
        extent = Some(extentProtoBufCodec.encode(extent.extent)),
        crs = Some(crsProtoBufCodec.encode(extent.crs)),
        instant = extent.instant
      )

    def decode(message: ProtoTemporalProjectedExtent): TemporalProjectedExtent =
      TemporalProjectedExtent(
        extentProtoBufCodec.decode(message.extent.get),
        crsProtoBufCodec.decode(message.crs.get),
        message.instant
      )
  }
*/
}
