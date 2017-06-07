package geopyspark.geotrellis

import scala.reflect._

import com.trueaccord.scalapb.GeneratedMessage

abstract class ProtoBufCodec[T: ClassTag] {
  type M <: GeneratedMessage

  def encode(thing: T): M
  def decode(message: M): T

  def supported[O](other: O): Boolean =
    implicitly[ClassTag[T]].unapply(other).isDefined
}

object ProToBufCodec {
  def apply[T: ProtoBufCodec]: ProtoBufCodec[T] = implicitly
}
