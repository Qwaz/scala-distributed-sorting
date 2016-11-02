package dsorting.serializer

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.net.InetSocketAddress

object InetSocketAddressSerializer extends Serializer[InetSocketAddress] {
  override def toByteArray(target: InetSocketAddress): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream()
    val objectStream = new ObjectOutputStream(byteStream)
    objectStream.writeUTF(target.getHostString)
    objectStream.writeInt(target.getPort)
    byteStream.toByteArray
  }

  override def fromByteArray(array: Array[Byte]): InetSocketAddress = {
    val byteStream = new ByteArrayInputStream(array)
    val objectStream = new ObjectInputStream(byteStream)

    val host = objectStream.readUTF()
    val port = objectStream.readInt()

    new InetSocketAddress(host, port)
  }
}
