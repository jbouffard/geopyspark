package geopyspark.geotrellis.protobufs

import geopyspark.geotrellis._
import geopyspark.geotrellis.protobufs.Implicits._
import protos.tileMessages._
import geotrellis.raster._


trait TileProtoBuf {
  def createTileInfo(tile: Tile): TileInfo =
    TileInfo(name = tile.cellType.name, cols = tile.cols, rows = tile.rows)

  implicit def bitTileProtoBufCodec = new ProtoBufCodec[BitArrayTile] {
    type M = UInt32Tile

    def encode(tile: BitArrayTile): UInt32Tile = {
      val info = Some(createTileInfo(tile))

      UInt32Tile(info = info, cells = tile.convert(IntCellType).toArray, noDataValue = None)
    }

    def decode(message: UInt32Tile): BitArrayTile =
      BitArrayTile(message.cells.map(_.toByte).toArray,
        message.info.get.cols,
        message.info.get.rows)
  }

  implicit def byteTileProtoBufCodec = new ProtoBufCodec[ByteArrayTile] {
    type M = Int32Tile

    def encode(tile: ByteArrayTile): Int32Tile = {
      val (noData, newCellType): (Option[NoData], CellType) =
        tile.cellType match {
          case ByteConstantNoDataCellType =>
            (Some(NoData().withInt32NoDataValue(NODATA)), IntConstantNoDataCellType)
          case ByteUserDefinedNoDataCellType(nd) =>
            (Some(NoData().withInt32NoDataValue(nd.toInt)), IntUserDefinedNoDataCellType(nd.toInt))
          case ByteCellType => (None, IntCellType)
          case _ => sys.error(s"Cell type ${tile.cellType} was unexpected")
        }

      val info = Some(createTileInfo(tile))

      Int32Tile(info = info, cells = tile.convert(newCellType).toArray, noDataValue = noData)
    }

    def decode(message: Int32Tile): ByteArrayTile = {
      val cellType = message.noDataValue match {
        case Some(noData) => {
          val nd = noData.getInt32NoDataValue.toByte
          if (isNoData(nd))
            ByteConstantNoDataCellType
          else
            ByteUserDefinedNoDataCellType(nd)
        }
        case None => ByteCellType
      }

      ByteArrayTile(message.cells.map(_.toByte).toArray,
        message.info.get.cols,
        message.info.get.rows,
        cellType)
    }
  }

  implicit def uByteTileProtoBufCodec = new ProtoBufCodec[UByteArrayTile] {
    type M = UInt32Tile

    def encode(tile: UByteArrayTile): UInt32Tile = {
      val (noData, newCellType): (Option[NoData], CellType) =
        tile.cellType match {
          case UByteConstantNoDataCellType =>
            (Some(NoData().withUInt32NoDataValue(NODATA)), IntConstantNoDataCellType)
          case UByteUserDefinedNoDataCellType(nd) =>
            (Some(NoData().withUInt32NoDataValue(nd.toInt)), IntUserDefinedNoDataCellType(nd.toInt))
          case UByteCellType => (None, IntCellType)
          case _ => sys.error(s"Cell type ${tile.cellType} was unexpected")
        }

      val info = Some(createTileInfo(tile))

      UInt32Tile(info = info, cells = tile.convert(newCellType).toArray, noDataValue = noData)
    }

    def decode(message: UInt32Tile): UByteArrayTile = {
      val cellType = message.noDataValue match {
        case Some(noData) => {
          val nd = noData.getUInt32NoDataValue.toByte
          if (isNoData(nd))
            UByteConstantNoDataCellType
          else
            UByteUserDefinedNoDataCellType(nd)
        }
        case None => UByteCellType
      }

      UByteArrayTile(message.cells.map(_.toByte).toArray,
        message.info.get.cols,
        message.info.get.rows,
        cellType)
    }
  }

  implicit def shortTileProtoBufCodec = new ProtoBufCodec[ShortArrayTile] {
    type M = Int32Tile

    def encode(tile: ShortArrayTile): Int32Tile = {
      val (noData, newCellType): (Option[NoData], CellType) =
        tile.cellType match {
          case ShortConstantNoDataCellType =>
            (Some(NoData().withInt32NoDataValue(NODATA)), IntConstantNoDataCellType)
          case ShortUserDefinedNoDataCellType(nd) =>
            (Some(NoData().withInt32NoDataValue(nd.toInt)), IntUserDefinedNoDataCellType(nd.toInt))
          case ShortCellType => (None, IntCellType)
          case _ => sys.error(s"Cell type ${tile.cellType} was unexpected")
        }

      val info = Some(createTileInfo(tile))

      Int32Tile(info = info, cells = tile.convert(newCellType).toArray, noDataValue = noData)
    }

    def decode(message: Int32Tile): ShortArrayTile = {
      val cellType = message.noDataValue match {
        case Some(noData) => {
          val nd = noData.getInt32NoDataValue.toShort
          if (isNoData(nd))
            ShortConstantNoDataCellType
          else
            ShortUserDefinedNoDataCellType(nd)
        }
        case None => ShortCellType
      }

      ShortArrayTile(message.cells.map(_.toShort).toArray,
        message.info.get.cols,
        message.info.get.rows,
        cellType)
    }
  }

  implicit def uShortTileProtoBufCodec = new ProtoBufCodec[UShortArrayTile] {
    type M = UInt32Tile

    def encode(tile: UShortArrayTile): UInt32Tile = {
      val (noData, newCellType): (Option[NoData], CellType) =
        tile.cellType match {
          case UShortConstantNoDataCellType =>
            (Some(NoData().withUInt32NoDataValue(NODATA)), IntConstantNoDataCellType)
          case UShortUserDefinedNoDataCellType(nd) =>
            (Some(NoData().withUInt32NoDataValue(nd.toInt)), IntUserDefinedNoDataCellType(nd.toInt))
          case UShortCellType => (None, IntCellType)
          case _ => sys.error(s"Cell type ${tile.cellType} was unexpected")
        }

      val info = Some(createTileInfo(tile))

      UInt32Tile(info = info, cells = tile.convert(newCellType).toArray, noDataValue = noData)
    }

    def decode(message: UInt32Tile): UShortArrayTile = {
      val cellType = message.noDataValue match {
        case Some(noData) => {
          val nd = noData.getUInt32NoDataValue.toShort
          if (isNoData(nd))
            UShortConstantNoDataCellType
          else
            UShortUserDefinedNoDataCellType(nd)
        }
        case None => UShortCellType
      }

      UShortArrayTile(message.cells.map(_.toShort).toArray,
        message.info.get.cols,
        message.info.get.rows,
        cellType)
    }
  }

  implicit def intTileProtoBufCodec = new ProtoBufCodec[IntArrayTile] {
    type M = Int32Tile

    def encode(tile: IntArrayTile): Int32Tile = {
      val noData: Option[NoData] =
        tile.cellType match {
          case IntConstantNoDataCellType =>
            Some(NoData().withInt32NoDataValue(NODATA))
          case IntUserDefinedNoDataCellType(nd) =>
            Some(NoData().withInt32NoDataValue(nd))
          case IntCellType => None
          case _ => sys.error(s"Cell type ${tile.cellType} was unexpected")
        }

      val info = Some(createTileInfo(tile))

      Int32Tile(info = info, cells = Array(tile.array:_*), noDataValue = noData)
    }

    def decode(message: Int32Tile): IntArrayTile = {
      val cellType = message.noDataValue match {
        case Some(noData) => {
          val nd = noData.getInt32NoDataValue
          if (isNoData(nd))
            IntConstantNoDataCellType
          else
            IntUserDefinedNoDataCellType(nd)
        }
        case None => IntCellType
      }

      IntArrayTile(message.cells.toArray,
        message.info.get.cols,
        message.info.get.rows,
        cellType)
    }
  }

  implicit def floatTileProtoBufCodec = new ProtoBufCodec[FloatArrayTile] {
    type M = FloatTile

    def encode(tile: FloatArrayTile): FloatTile = {
      val noData: Option[NoData] =
        tile.cellType match {
          case FloatConstantNoDataCellType =>
            Some(NoData().withFloatNoDataValue(NODATA))
          case FloatUserDefinedNoDataCellType(nd) =>
            Some(NoData().withFloatNoDataValue(nd))
          case FloatCellType => None
          case _ => sys.error(s"Cell type ${tile.cellType} was unexpected")
        }

      val info = Some(createTileInfo(tile))

      FloatTile(info = info, cells = Array(tile.array:_*), noDataValue = noData)
    }

    def decode(message: FloatTile): FloatArrayTile = {
      val cellType = message.noDataValue match {
        case Some(noData) => {
          val nd = noData.getFloatNoDataValue
          if (isNoData(nd))
            FloatConstantNoDataCellType
          else
            FloatUserDefinedNoDataCellType(nd)
        }
        case None => FloatCellType
      }

      FloatArrayTile(message.cells.toArray,
        message.info.get.cols,
        message.info.get.rows,
        cellType)
    }
  }

  implicit def doubleTileProtoBufCodec = new ProtoBufCodec[DoubleArrayTile] {
    type M = DoubleTile

    def encode(tile: DoubleArrayTile): DoubleTile = {
      val noData: Option[NoData] =
        tile.cellType match {
          case DoubleConstantNoDataCellType =>
            Some(NoData().withDoubleNoDataValue(NODATA))
          case DoubleUserDefinedNoDataCellType(nd) =>
            Some(NoData().withDoubleNoDataValue(nd))
          case DoubleCellType => None
          case _ => sys.error(s"Cell type ${tile.cellType} was unexpected")
        }

      val info = Some(createTileInfo(tile))

      DoubleTile(info = info, cells = Array(tile.array:_*), noDataValue = noData)
    }

    def decode(message: DoubleTile): DoubleArrayTile = {
      val cellType = message.noDataValue match {
        case Some(noData) => {
          val nd = noData.getDoubleNoDataValue
          if (isNoData(nd))
            DoubleConstantNoDataCellType
          else
            DoubleUserDefinedNoDataCellType(nd)
        }
        case None => DoubleCellType
      }

      DoubleArrayTile(message.cells.toArray,
        message.info.get.cols,
        message.info.get.rows,
        cellType)
    }
  }
}


trait MultibandTileProtoBuf {
  implicit def multiBandTileProtoBufCodec = new ProtoBufCodec[MultibandTile] {
    type M = Multiband

    def encode(tile: MultibandTile): Multiband = ???
    def decode(message: Multiband): MultibandTile = ???

    /*
    def encode(tile: MultibandTile): Multiband = {
      val bands = for (i <- 0 until tile.bandCount) yield.band(i)
      val noData: Option[NoData] =
        tile.cellType match {
          case DoubleConstantNoDataCellType =>
            Some(NoData().withDoubleNoDataValue(NODATA))
          case DoubleUserDefinedNoDataCellType(nd) =>
            Some(NoData().withDoubleNoDataValue(nd))
          case MultibandCellType => None
          case _ => sys.error(s"Cell type ${tile.cellType} was unexpected")
        }

      val info = Some(createTileInfo(tile))

      Multiband(info = info, cells = Array(tile.array:_*), noDataValue = noData)
    }

    def decode(message: Multiband): MultibandTile = {
      val cellType = message.noDataValue match {
        case Some(noData) => {
          val nd = noData.getDoubleNoDataValue
          if (isNoData(nd))
            DoubleConstantNoDataCellType
          else
            DoubleUserDefinedNoDataCellType(nd)
        }
        case None => MultibandCellType
      }

      MultibandTile(message.cells.toArray,
        message.info.get.cols,
        message.info.get.rows,
        cellType)
    }
*/
  }
}
