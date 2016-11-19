package dsorting.serializer

import java.io.ByteArrayInputStream

import dsorting.primitive._

object KeyListSerializer extends Serializer[List[Key]] {
  override def toByteArray(target: List[Key]): Array[Byte] = {
    target.foldLeft(Array[Byte]()) {
      (acc, key) => acc ++ key.bytes
    }
  }

  override def fromByteArray(array: Array[Byte]): List[Key] = {
    def readKey(stream: ByteArrayInputStream): List[Key] = {
      if (stream.available() <= 0) Nil
      else {
        val newKeyBuffer = emptyKeyBuffer
        stream.read(newKeyBuffer, 0, 10)
        new Key(newKeyBuffer) :: readKey(stream)
      }
    }

    val stream = new ByteArrayInputStream(array)
    val keyList = readKey(stream)

    stream.close()

    keyList
  }
}
