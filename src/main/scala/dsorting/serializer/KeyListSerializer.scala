package dsorting.serializer

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import dsorting.primitive._

object KeyListSerializer extends Serializer[Seq[Key]] {
  override def toByteArray(target: Seq[Key]): Array[Byte] = {
    val stream = new ByteArrayOutputStream()

    target.foreach(key => stream.write(key.bytes))

    stream.close()

    stream.toByteArray
  }

  override def fromByteArray(array: Array[Byte]): Seq[Key] = {
    def readKey(stream: ByteArrayInputStream): Seq[Key] = {
      if (stream.available() <= 0) Nil
      else {
        val keyBuffer = BufferFactory.emptyKeyBuffer()
        stream.read(keyBuffer, 0, 10)
        new Key(keyBuffer) +: readKey(stream)
      }
    }

    val stream = new ByteArrayInputStream(array)
    val keyList = readKey(stream)

    stream.close()

    keyList
  }
}
