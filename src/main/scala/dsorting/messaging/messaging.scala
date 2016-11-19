package dsorting

import java.io.DataOutputStream
import java.net.{InetSocketAddress, Socket, SocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}

import com.typesafe.scalalogging.Logger
import dsorting.future._
import dsorting.primitive._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

package object messaging {
  def messagingLogging(log: String) = {
    if (Setting.MessageLoggingEnabled) {
      val messageLogger = Logger("Messaging")
      messageLogger.debug(log)
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
        stream.writeByte(message.messageType.id)
        stream.write(message.data)
        messagingLogging(s"Sent ${message.data.length + 1} bytes")
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

  trait MessageHandler {
    def handleMessage(message: Message): Unit
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

    private var messageHandler: MessageHandler = new MessageHandler {
      def handleMessage(message: Message) = {
        ()
      }
    }

    def replaceHandler(handler: MessageHandler) = {
      messageHandler = handler
    }

    def startServer = {
      serverChannel.register(selector, SelectionKey.OP_ACCEPT)

      Future.run() {
        ct => Future {
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
            messagingLogging(s"Read $readBytes bytes")
            buffer.flip()
            messageHandler.handleMessage(readMessageFrom(buffer, readBytes))
          }
      }
    }

    private def readMessageFrom(buffer: ByteBuffer, readBytes: Integer) = {
      val messageType = MessageType(buffer.get())
      val data = new Array[Byte](readBytes-1)
      buffer.get(data)
      new Message(messageType, data)
    }
  }
}