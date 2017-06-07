package geopyspark.geotrellis.protobufs

import geopyspark.geotrellis._
import geotrellis.vector._
import protos.extentMessages.{Extent => ProtoExtent}


trait ExtentProtoBuf {
  implicit def extentProtoBufCodec = new ProtoBufCodec[Extent] {
    type M = ProtoExtent

    def encode(extent: Extent): ProtoExtent =
      ProtoExtent(
        xmin = extent.xmin,
        ymin = extent.ymin,
        xmax = extent.xmax,
        ymax = extent.ymax)

    def decode(message: ProtoExtent): Extent =
      Extent(message.xmin, message.ymin, message.xmax, message.ymax)
  }
}
