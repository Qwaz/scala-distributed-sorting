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
    def readKey(stream: ByteArrayInputStream, now: List[Key]): List[Key] = {
      if (stream.available() <= 0) now
      else {
        val newKeyBuffer = emptyKeyBuffer
        stream.read(newKeyBuffer, 0, 10)
        readKey(stream, new Key(newKeyBuffer) :: now)
      }
    }

    val stream = new ByteArrayInputStream(array)
    val keyList = readKey(stream, Nil)

    stream.close()

    keyList
  }
}
