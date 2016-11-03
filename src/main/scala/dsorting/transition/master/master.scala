package dsorting.transition

import java.net.{InetAddress, InetSocketAddress}
import java.nio.charset.Charset

import com.typesafe.scalalogging.Logger
import dsorting.common.Setting
import dsorting.common.future.Subscription
import dsorting.common.messaging._
import dsorting.common.primitive._
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
    new SamplingState {
      val logger = Logger("Master Sampling")

      private val masterAddress = new InetSocketAddress(InetAddress.getLocalHost.getHostAddress, Setting.MasterPort)
      logger.debug(s"master address: $masterAddress")

      val listener = new MessageListener(masterAddress)
      val serverSubscription: Subscription = listener.startServer

      val numSlaves = _numSlaves

      private var remainingSlaves = numSlaves

      private val slaveAddresses = ArrayBuffer[InetSocketAddress]()
      private val sampleKeys = ArrayBuffer[Key]()

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
          if (remainingSlaves == 0) p.success(createPartitionTable())
          logger.debug(s"sample data received: $remainingSlaves remains")
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

  def preparePartitioning(prevState: SamplingState)(partitionTable: PartitionTable): PartitioningState = {
    val _partitionTable = partitionTable
    new PartitioningState {
      val logger = Logger("Master Partitioning")

      val listener = prevState.listener
      val serverSubscription: Subscription = prevState.serverSubscription

      val numSlaves = prevState.numSlaves

      val partitionTable = _partitionTable
      val channelTable = ChannelTable.fromPartitionTable(partitionTable)

      private var remainingSlaves = numSlaves

      def run() = {
        logger.info("start running")

        val p = Promise[Unit]()

        def receivePartitionOk(data: Array[Byte]) = {
          remainingSlaves -= 1
          if (remainingSlaves == 0) {
            channelTable.broadcast(Message.withType(MessageType.PartitionComplete))
            p.success(())
          }
          logger.debug(s"partitioning ok received: $remainingSlaves remains")
        }

        listener.replaceHandler(new MessageHandler {
          def handleMessage(message: Message): Unit =  message.messageType match {
            case MessageType.PartitionOk => receivePartitionOk(message.data)
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