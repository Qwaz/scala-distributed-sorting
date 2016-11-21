package dsorting.transition

import java.net.InetSocketAddress
import java.nio.charset.Charset

import com.typesafe.scalalogging.Logger
import dsorting.Setting
import dsorting.messaging._
import dsorting.primitive._
import dsorting.serializer._
import dsorting.states.master._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

package object master {
  def parseArgument(args: Array[String]): Future[Integer] = Future {
    if (args.length != 1) throw new IllegalArgumentException("Argument size must be 1")
    args(0).toInt
  }

  def prepareSampling(numSlaves: Integer): SamplingState = {
    val _numSlaves = numSlaves
    new FreshState(Setting.MasterPort) with SamplingState {
      val logger = Logger("Master Sampling")

      val numSlaves = _numSlaves
      logger.debug(s"numSlaves $numSlaves")

      private var remainingSlaves = numSlaves

      private val slaveAddresses = ArrayBuffer[InetSocketAddress]()
      private val sampleKeys = ArrayBuffer[Key](Key(Array[Byte](0, 0, 0, 0, 0, 0, 0, 0, 0, 0)))

      def run() = {
        logger.info("start running")

        val p = Promise[PartitionTable]()

        def receiveIntroduce(data: Array[Byte]) = {
          val slaveAddress = InetSocketAddressSerializer.fromByteArray(data)
          slaveAddresses += slaveAddress
          logger.debug(s"introduce data received from $slaveAddress")
        }

        def receiveSampleData(data: Array[Byte]) = {
          sampleKeys ++= KeyListSerializer.fromByteArray(data)
          remainingSlaves -= 1
          logger.debug(s"sample data received: $remainingSlaves remains")
          if (remainingSlaves == 0) p.success(createPartitionTable())
        }

        listener.replaceHandler(new MessageHandler {
          def handleMessage(message: Message): Unit = message.messageType match {
            case MessageType.Introduce => receiveIntroduce(message.data)
            case MessageType.SampleData => receiveSampleData(message.data)
            case _ => ()
          }
        })

        p.future
      }

      private def createPartitionTable() = {
        val sortedKeys = sampleKeys.sortBy(key => new String(key.bytes, Charset.forName("US-ASCII")))
        val step = sortedKeys.length / numSlaves
        val slaveRanges = slaveAddresses.zipWithIndex.map {
          case (socketAddress, i) => new SlaveRange(socketAddress, sortedKeys(i*step))
        }
        new PartitionTable(Master, slaveRanges.toVector)
      }

      logger.info("initialized")
    }
  }

  def prepareShuffling(prevState: SamplingState)(partitionTable: PartitionTable): ShufflingState = {
    val _partitionTable = partitionTable
    new TransitionFrom(prevState) with ShufflingState {
      val logger = Logger("Master Shuffling")

      val partitionTable = _partitionTable
      logger.debug(partitionTable.toString)
      val channelTable = ChannelTable.fromPartitionTable(partitionTable)

      private var remainingSlaves = numSlaves

      def run() = {
        logger.info("start running")

        val p = Promise[Unit]()

        def receiveShufflingReady(data: Array[Byte]) = {
          remainingSlaves -= 1
          logger.debug(s"shuffling ready received: $remainingSlaves remains")
          if (remainingSlaves == 0) {
            channelTable.broadcast(Message.withType(MessageType.ShufflingStart))
            p.success(())
          }
        }

        listener.replaceHandler(new MessageHandler {
          def handleMessage(message: Message): Unit =  message.messageType match {
            case MessageType.ShufflingReady => receiveShufflingReady(message.data)
            case _ => ()
          }
        })

        broadcastPartitionTable()

        p.future
      }

      private def broadcastPartitionTable() = {
        channelTable.channels.zipWithIndex.foreach {
          case (channel, i) =>
            channel.sendMessage(new Message(
              MessageType.PartitionData,
              PartitionTableSerializer.toByteArray(
                new PartitionTable(Slave(i), partitionTable.slaveRanges)
              )
            ))
        }
      }

      logger.info("initialized")
    }
  }
}