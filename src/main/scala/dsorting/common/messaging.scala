package dsorting.common

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.net.{InetSocketAddress, Socket, SocketAddress}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}

import com.typesafe.scalalogging.Logger
import dsorting.common.future._
import dsorting.common.primitive._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

package object messaging {
  object MessageType extends Enumeration {
    /*
     MessageType is serialized to one byte, so the number of values should be less than 256.
      */
    type MessageType = Value

    // Connection
    val Introduce = Value

    // Sampling
    val SampleData = Value

    // Partitioning
    val PartitionData = Value
    val PartitionOk = Value
    val PartitionComplete = Value

    // Shuffling
    val ShufflingData = Value
    val ShufflingOk = Value
    val ShufflingComplete = Value

    // Sorting
    val SortingOk = Value
    val SortingComplete = Value
  }

  class Message(val messageType: MessageType.MessageType, val data: Array[Byte])

  class Channel(val identity: Identity, address: SocketAddress) {
    private val socket = new Socket
    socket.connect(address)
    private val stream = socket.getOutputStream

    def sendMessage(message: Message): Future[Unit] = Future {
      stream.synchronized {
        stream.write(message.messageType.id)
        stream.write(message.data)
        stream.flush()
      }
    }
  }

  trait MessageHandler {
    def handleMessage(message: Message): Future[Unit]
  }

  class MessageListener(bindAddress: InetSocketAddress) {
    val logger = Logger("Listener")

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
      def handleMessage(message: Message) = Future {
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
              } else if (key.isReadable) {
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
          registerChannel(selector, socketChannel)
      }
    }

    private def read(key: SelectionKey) = {
      key.channel match {
        case socketChannel: SocketChannel =>
          val buffer = ByteBuffer.allocateDirect(Setting.BufferSize)
          val readedBytes = socketChannel.read(buffer)
          buffer.flip()
          messageHandler.handleMessage(readMessageFrom(buffer, readedBytes))
          buffer.clear
      }
    }

    private def registerChannel(selector: Selector, socketChannel: SocketChannel) = {
      socketChannel.configureBlocking(false)
      socketChannel.register(selector, SelectionKey.OP_READ)
    }

    private def readMessageFrom(buffer: ByteBuffer, readedBytes: Integer) = {
      val messageType = MessageType(buffer.get())
      val data = new Array[Byte](readedBytes-1)
      buffer.get(data)
      new Message(messageType, data)
    }
  }
}