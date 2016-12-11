package dsorting.messaging

import java.io.{ByteArrayOutputStream, DataOutputStream, PrintWriter, StringWriter}
import java.net.{InetSocketAddress, Socket, SocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}

import com.typesafe.scalalogging.Logger
import dsorting.Setting
import dsorting.future._
import dsorting.primitive._

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, blocking}

object MessageLogger {
  def log(msg: String): Unit = {
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
  override def toString: String = {
    s"$messageType - $data"
  }

  def toBytes: Array[Byte] = {
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
  def withType(messageType: MessageType.MessageType): Message = {
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
  def apply(index: Int): Channel = channels(index)

  def broadcast(message: Message): Unit = {
    channels.foreach { channel => channel.sendMessage(message) }
  }
}

object ChannelTable {
  def fromPartitionTable(partitionTable: PartitionTable): ChannelTable = {
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

  type MessageHandler = Message => Future[Unit]
  private var messageHandler: MessageHandler = {
    _ => Future()
  }

  def replaceHandler(handler: MessageHandler): Unit = {
    messageHandler = handler
  }

  def start(): Subscription = {
    serverChannel.register(selector, SelectionKey.OP_ACCEPT)

    val serverSubscription = runServer()

    serverSubscription
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

  private val hashMap = mutable.HashMap.empty[SocketChannel, ByteBuffer]

  private def accept(key: SelectionKey) = {
    key.channel match {
      case server: ServerSocketChannel =>
        val socketChannel = server.accept
        socketChannel.configureBlocking(false)
        socketChannel.register(selector, SelectionKey.OP_READ)
        hashMap += (socketChannel -> ByteBuffer.allocateDirect(Setting.BufferSize))
    }
  }

  private def read(key: SelectionKey) = {
    key.channel match {
      case socketChannel: SocketChannel =>
        val buffer = hashMap(socketChannel)
        val readBytes = socketChannel.read(buffer)
        if (readBytes == -1) {
          hashMap -= socketChannel
          socketChannel.close()
        } else {
          MessageLogger.log(s"Read $readBytes bytes")
          var message = readMessage(buffer)
          while (message.isDefined) {
            messageHandler(message.get) onFailure {
              case e =>
                val logger = Logger("Message Handling Failed")
                val sw = new StringWriter
                e.printStackTrace(new PrintWriter(sw))
                logger.error(sw.toString)
            }
            message = readMessage(buffer)
          }
        }
    }
  }

  private def readMessage(buffer: ByteBuffer): Option[Message] = {
    val dup = buffer.duplicate()
    dup.flip()
    if (dup.remaining >= 5) {
      val messageType = dup.get()
      val messageLength = dup.getInt()
      if (dup.remaining >= messageLength) {
        val data = new Array[Byte](messageLength)
        dup.get(data)
        buffer.position(dup.position)
        buffer.limit(dup.limit)
        buffer.compact()
        Some(new Message(MessageType(messageType), data))
      } else None
    } else None
  }
}