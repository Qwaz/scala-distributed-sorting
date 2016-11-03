package dsorting.serializer

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.net.InetSocketAddress

object InetSocketAddressSerializer extends Serializer[InetSocketAddress] {
  override def toByteArray(target: InetSocketAddress): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream()
    val objectStream = new ObjectOutputStream(byteStream)
    objectStream.writeUTF(target.getAddress.getHostAddress)
    objectStream.writeInt(target.getPort)

    objectStream.close()
    byteStream.close()

    byteStream.toByteArray
  }

  override def fromByteArray(array: Array[Byte]): InetSocketAddress = {
    val byteStream = new ByteArrayInputStream(array)
    val objectStream = new ObjectInputStream(byteStream)

    val host = objectStream.readUTF()
    val port = objectStream.readInt()

    objectStream.close()
    byteStream.close()

    new InetSocketAddress(host, port)
  }
}
