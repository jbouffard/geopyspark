package geopyspark.geotrellis.protobufs

import geopyspark.geotrellis._
import geopyspark.geotrellis.protobufs.Implicits._
import geotrellis.vector._

import protos.extentMessages.{ProjectedExtent => ProtoProjectedExtent}


trait ProjectedExtentProtoBuf {
  implicit def projectedExtentProtoBufFormat = new ProtoBufCodec[ProjectedExtent] {
    type M = ProtoProjectedExtent

    def encode(extent: ProjectedExtent): ProtoProjectedExtent =
      ProtoProjectedExtent(
        extent = Some(extentProtoBufCodec.encode(extent.extent)),
        crs = Some(crsProtoBufCodec.encode(extent.crs))
      )

    def decode(message: ProtoProjectedExtent): ProjectedExtent =
      ProjectedExtent(
        extentProtoBufCodec.decode(message.extent.get),
        crsProtoBufCodec.decode(message.crs.get)
      )
  }
}
