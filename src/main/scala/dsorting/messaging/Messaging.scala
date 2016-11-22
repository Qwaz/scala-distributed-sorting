package dsorting.messaging

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.net.{InetSocketAddress, Socket, SocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}

import com.typesafe.scalalogging.Logger
import dsorting.Setting
import dsorting.future._
import dsorting.primitive._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, blocking}

object MessageLogger {
  def log(msg: String) = {
    if (Setting.MessageLoggingEnabled) {
      Logger("Messaging").debug(msg)
    }
  }
}

object MessageType extends Enumeration {
  /*
   MessageType is serialized to one byte, so the number of values should be less than 256.
    */
  type MessageType = Value

  // Sampling
  val Introduce = Value
  val SampleData = Value
  val PartitionData = Value

  // Shuffling
  val ShufflingReady = Value
  val ShufflingStart = Value
  val Shuffle = Value
  val ShufflingDone = Value
  val ShufflingComplete = Value

  // Sorting
  val SortingDone = Value
  val SortingComplete = Value
}

class Message(val messageType: MessageType.MessageType, val data: Array[Byte]) {
  override def toString = {
    s"$messageType - $data"
  }

  def toBytes = {
    val byteStream = new ByteArrayOutputStream()
    val dataStream = new DataOutputStream(byteStream)

    dataStream.writeByte(messageType.id)
    dataStream.writeInt(data.length)
    dataStream.write(data)

    dataStream.close()
    byteStream.close()
    byteStream.toByteArray
  }
}

object Message {
  def withType(messageType: MessageType.MessageType) = {
    new Message(messageType, Array[Byte]())
  }
}

class Channel(val identity: Identity, address: SocketAddress) {
  private val socket = new Socket
  socket.connect(address)
  private val stream = new DataOutputStream(socket.getOutputStream)

  def sendMessage(message: Message): Future[Unit] = Future {
    stream.synchronized {
      val bytes = message.toBytes
      stream.write(bytes)
      MessageLogger.log(s"Sent ${bytes.length} bytes")
      stream.flush()
    }
  }
}

class ChannelTable(val channels: IndexedSeq[Channel]) {
  def apply(index: Integer) = channels(index)

  def broadcast(message: Message) = {
    channels.foreach { channel => channel.sendMessage(message) }
  }
}

object ChannelTable {
  def fromPartitionTable(partitionTable: PartitionTable) = {
    val channels = partitionTable.slaveRanges.zipWithIndex.map {
      case (slaveRange, i) =>
        new Channel(Slave(i), slaveRange.slave)
    }
    new ChannelTable(channels)
  }
}

class MessageListener(bindAddress: InetSocketAddress) {
  /*
  http://zeroit.tistory.com/236
  Java non-blocking socket I/O
   */
  private val selector = Selector.open
  private val serverChannel = ServerSocketChannel.open
  serverChannel.configureBlocking(false)

  private val socket = serverChannel.socket
  socket.bind(bindAddress)

  type MessageHandler = Message => Unit
  private var messageHandler: MessageHandler = {
    _ => ()
  }

  def replaceHandler(handler: MessageHandler) = {
    messageHandler = handler
  }

  var remainBytes = ArrayBuffer[Byte]()

  def start() = {
    serverChannel.register(selector, SelectionKey.OP_ACCEPT)

    val serverSubscription = runServer()
    val messageConsumerSubscription = runMessageConsumer()

    Subscription(serverSubscription, messageConsumerSubscription)
  }

  private def runServer() = {
    Future.run() {
      ct => Future {
        blocking {
          while (ct.nonCancelled) {
            selector.select

            val it = selector.selectedKeys.iterator
            while (it.hasNext) {
              val key = it.next
              if (key.isAcceptable) {
                accept(key)
              }
              if (key.isReadable) {
                read(key)
              }
              it.remove()
            }
          }
          socket.close()
        }
      }
    }
  }

  private def runMessageConsumer() = {
    def tryReadMessage(): Option[Message] = {
      val Overhead = 5
      if (remainBytes.length >= Overhead) {
        val lengthBuffer = new Array[Byte](Overhead)
        remainBytes.synchronized {
          remainBytes.copyToArray(lengthBuffer, 0, Overhead)

          //TODO: update this part for performance improvement
          val byteStream = new ByteArrayInputStream(lengthBuffer)
          val dataStream = new DataInputStream(byteStream)

          val messageType = dataStream.readByte()
          val dataLength = dataStream.readInt()

          dataStream.close()
          byteStream.close()

          if (remainBytes.length >= Overhead + dataLength) {
            val dataBuffer = new Array[Byte](dataLength)
            remainBytes.remove(0, Overhead)
            remainBytes.copyToArray(dataBuffer, 0, dataLength)
            remainBytes.remove(0, dataLength)
            Some(new Message(MessageType(messageType), dataBuffer))
          } else None
        }
      } else None
    }

    Future.run() {
      ct => Future {
        blocking {
          while (ct.nonCancelled) {
            tryReadMessage() match {
              case Some(message) => messageHandler(message)
              case _ => ()
            }
          }
        }
      }
    }
  }

  private def accept(key: SelectionKey) = {
    key.channel match {
      case server: ServerSocketChannel =>
        val socketChannel = server.accept
        socketChannel.configureBlocking(false)
        socketChannel.register(selector, SelectionKey.OP_READ)
    }
  }

  private def read(key: SelectionKey) = {
    key.channel match {
      case socketChannel: SocketChannel =>
          val buffer = ByteBuffer.allocateDirect(Setting.BufferSize)
          val readBytes = socketChannel.read(buffer)
          if (readBytes == -1) {
            socketChannel.close()
          } else {
            MessageLogger.log(s"Read $readBytes bytes")
            buffer.flip()
            val bytes = new Array[Byte](buffer.remaining())
            buffer.get(bytes)
            remainBytes.synchronized {
              remainBytes.appendAll(bytes)
            }
          }
    }
  }
}