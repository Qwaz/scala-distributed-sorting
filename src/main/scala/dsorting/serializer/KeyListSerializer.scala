package dsorting.serializer

import java.io.ByteArrayInputStream

import dsorting.common.primitive._

object KeyListSerializer extends Serializer[List[Key]] {
  override def toByteArray(target: List[Key]): Array[Byte] = {
    target.reduceLeft {(left, right) => left ++ right}
  }

  override def fromByteArray(array: Array[Byte]): List[Key] = {
    def readKey(stream: ByteArrayInputStream, pos: Integer, now: List[Key]): List[Key] = {
      if (stream.available() <= pos) now
      else {
        val newKey = Array[Byte](10)
        stream.read(newKey, pos, 10)
        readKey(stream, pos+10, newKey :: now)
      }
    }
    val stream = new ByteArrayInputStream(array)
    readKey(stream, 0, Nil)
  }
}
