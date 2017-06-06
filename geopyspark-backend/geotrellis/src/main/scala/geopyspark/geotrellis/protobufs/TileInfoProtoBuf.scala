package geopyspark.geotrellis.protobufs

import protos.tileMessages.TileInfo
import geotrellis.raster._

trait TileInfoProtoBuf {
  def encodeTileInfo(tile: Tile): TileInfo =
    tile match {
      case _: BitArrayTile => TileInfo(name = "BitArrayTile", cols = tile.cols, rows = tile.rows)
      case _: ByteArrayTile => TileInfo(name = "ByteArrayTile", cols = tile.cols, rows = tile.rows)
      case _: UByteArrayTile => TileInfo(name = "UByteArrayTile", cols = tile.cols, rows = tile.rows)
      case _: ShortArrayTile => TileInfo(name = "ShortArrayTile", cols = tile.cols, rows = tile.rows)
      case _: UShortArrayTile => TileInfo(name = "UShortArrayTile", cols = tile.cols, rows = tile.rows)
      case _: IntArrayTile => TileInfo(name = "IntArrayTile", cols = tile.cols, rows = tile.rows)
      case _: FloatArrayTile => TileInfo(name = "FloatArrayTile", cols = tile.cols, rows = tile.rows)
      case _: DoubleArrayTile => TileInfo(name = "DoubleArrayTile", cols = tile.cols, rows = tile.rows)
    }
}
