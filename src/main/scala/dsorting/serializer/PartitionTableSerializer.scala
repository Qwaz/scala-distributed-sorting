package dsorting.serializer

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.net.InetSocketAddress

import dsorting.common.primitive._

import scala.collection.mutable.ArrayBuffer

object PartitionTableSerializer extends Serializer[PartitionTable] {
  override def toByteArray(target: PartitionTable): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream()
    val objectStream = new ObjectOutputStream(byteStream)

    target.identity match {
      case Master => objectStream.writeInt(-1)
      case Slave(i) => objectStream.writeInt(i)
    }

    objectStream.writeInt(target.slaveRanges.length)

    for (slaveRange <- target.slaveRanges) {
      objectStream.writeUTF(slaveRange.slave.getAddress.getHostAddress)
      objectStream.writeInt(slaveRange.slave.getPort)
      objectStream.writeObject(slaveRange.startKey.bytes)
    }

    objectStream.close()
    byteStream.close()

    byteStream.toByteArray
  }

  override def fromByteArray(array: Array[Byte]): PartitionTable = {
    val byteStream = new ByteArrayInputStream(array)
    val objectStream = new ObjectInputStream(byteStream)

    val id = objectStream.readInt()
    val identity = if (id == -1) Master else Slave(id)

    val length = objectStream.readInt()
    val slaveRanges = ArrayBuffer[SlaveRange]()

    for (idx <- 0 until length) {
      val host = objectStream.readUTF()
      val port = objectStream.readInt()
      val keyBytes = objectStream.readObject().asInstanceOf[Array[Byte]]
      slaveRanges += new SlaveRange(new InetSocketAddress(host, port), Key(keyBytes))
    }

    new PartitionTable(identity, slaveRanges)
  }
}
