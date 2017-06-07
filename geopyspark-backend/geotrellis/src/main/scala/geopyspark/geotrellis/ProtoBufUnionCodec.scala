package geopyspark.geotrellis

import scala.reflect._


class ProtoBufUnionCodec[T: ClassTag](formats: ProtoBufCodec[X] forSome {type X <: T} *) extends ProtoBufCodec[T] {
  override def encode(thing: T): M = {
    val format = findFormat(_.supported(thing), thing.getClass.toString)
    format.encode(thing)
  }

  def decode(message: M): T = ???

  def findFormat(f: ProtoBufCodec[_] => Boolean, target: String): ProtoBufCodec[T] =
    formats.filter(f) match {
      case Seq(format) => format.asInstanceOf[ProtoBufCodec[T]]
      case _ => throw new Error
    }
}
