package dsorting.serializer

import java.io._
import java.net.InetSocketAddress

object InetSocketAddressSerializer extends Serializer[InetSocketAddress] {
  override def toByteArray(target: InetSocketAddress): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream()
    val dataStream = new DataOutputStream(byteStream)
    dataStream.writeUTF(target.getAddress.getHostAddress)
    dataStream.writeInt(target.getPort)

    dataStream.close()
    byteStream.close()

    byteStream.toByteArray
  }

  override def fromByteArray(array: Array[Byte]): InetSocketAddress = {
    val byteStream = new ByteArrayInputStream(array)
    val dataStream = new DataInputStream(byteStream)

    val host = dataStream.readUTF()
    val port = dataStream.readInt()

    dataStream.close()
    byteStream.close()

    new InetSocketAddress(host, port)
  }
}
