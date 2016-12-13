package dsorting.serializer

import java.io._

import dsorting.states.slave.ShufflingData

object ShufflingDataSerializer extends Serializer[ShufflingData] {
  override def toByteArray(target: ShufflingData): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream()
    val dataStream = new DataOutputStream(byteStream)

    dataStream.writeInt(target.fromSlave)
    dataStream.write(target.data)

    dataStream.close()
    byteStream.close()

    byteStream.toByteArray
  }

  override def fromByteArray(array: Array[Byte]): ShufflingData = {
    val byteStream = new ByteArrayInputStream(array)
    val dataStream = new DataInputStream(byteStream)

    val fromSlave = dataStream.readInt()
    val data = new Array[Byte](array.length - 4)
    dataStream.read(data)

    dataStream.close()
    byteStream.close()

    new ShufflingData(fromSlave, data)
  }
}
