package geopyspark.geotrellis.protobufs

import geopyspark.geotrellis._
import geopyspark.geotrellis.protobufs.Implicits._
import protos.tileMessages._
import geotrellis.raster._


trait IntTileProtoBuf {
/*
  implicit def intTileProtoBufCodec = new ProtoBufCodec[IntArrayTile, IntTile] {
    def encode(tile: IntArrayTile): IntTile = {
      val noData: Integer =
        tile.cellType match {
          case IntConstantNoDataCellType => NODATA
          case IntUserDefinedNoDataCellType(nd) => nd
          case IntCellType => null
          case _ => sys.error(s"Cell type ${tile.cellType} was unexpected")
        }

      val int32Tile = Int32Tile(info = Some(encodeTileInfo(tile)), cells = Array(tile.array:_*), noDataValue = noData)

      IntTile(tile = Some(int32Tile))
    }

    def decode(message: IntTile): IntArrayTile = {
      val cellType = message.tile.get.noDataValue match {
        case nd if isNoData(nd) => IntConstantNoDataCellType
        case nd => IntUserDefinedNoDataCellType(nd)
        case _ => IntCellType
      }

      IntArrayTile(message.tile.get.cells.toArray,
        message.tile.get.info.get.cols,
        message.tile.get.info.get.rows,
        cellType)
    }
  }
*/
}

/*
trait BitTileProtoBuf {
  implicit def bitTileProtoBufCodec = new ProtoBufCodec[BitArrayTile, BitTile] {
    def encode(tile: BitArrayTile): BitTile =
      BitTile(info = Some(encodeTileInfo(tile)), cells = tile.array.asInstanceOf[Array[Int]])

    def decode(message: BitTile): BitArrayTile =
      BitArrayTile(message.cells.toArray.asInstanceOf[Array[Byte]], message.info.get.cols, message.info.get.rows)
  }
}
*/
