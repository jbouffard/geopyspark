package geopyspark.geotrellis

import scala.reflect._

import com.trueaccord.scalapb.Message
import com.trueaccord.scalapb.GeneratedMessage
import com.trueaccord.scalapb.GeneratedMessageCompanion
import com.google.protobuf.AbstractMessage


/*
abstract class ProtoBuffCodec[T, M] {
  def encode(thing: T): M
  def decode(message: M): T
}
*/

abstract class ProtoBufCodec[T: ClassTag, M <: GeneratedMessage] {
  def encode(thing: T): M
  def decode(message: M): T
}
