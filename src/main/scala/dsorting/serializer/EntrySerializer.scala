package dsorting.serializer

import java.io.ByteArrayInputStream

import dsorting.Setting
import dsorting.primitive._

object EntrySerializer extends Serializer[Entry] {
  override def toByteArray(target: Entry): Array[Byte] = {
    target.key.bytes ++ target.value.bytes
  }

  override def fromByteArray(array: Array[Byte]): Entry = {
    val byteStream = new ByteArrayInputStream(array)

    val keyBuffer = BufferFactory.emptyKeyBuffer()
    byteStream.read(keyBuffer, 0, Setting.KeySize)

    val valueBuffer = BufferFactory.emptyValueBuffer()
    byteStream.read(valueBuffer, 0, Setting.ValueSize)

    byteStream.close()

    Entry(
      new Key(keyBuffer),
      new Value(valueBuffer)
    )
  }
}
