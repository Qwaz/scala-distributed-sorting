package dsorting.serializer

import java.io.ByteArrayInputStream

import dsorting.common.primitive._

object KeyListSerializer extends Serializer[List[Key]] {
  override def toByteArray(target: List[Key]): Array[Byte] = {
    target.foldLeft(Array[Byte]()) {
      (acc, key) => acc ++ key.bytes
    }
  }

  override def fromByteArray(array: Array[Byte]): List[Key] = {
    def readKey(stream: ByteArrayInputStream, pos: Integer, now: List[Key]): List[Key] = {
      if (stream.available() <= pos) now
      else {
        val newKeyBuffer = emptyKeyBuffer
        stream.read(newKeyBuffer, pos, 10)
        readKey(stream, pos+10, new Key(newKeyBuffer) :: now)
      }
    }

    val stream = new ByteArrayInputStream(array)
    val keyList = readKey(stream, 0, Nil)

    stream.close()

    keyList
  }
}
