package dsorting.transition

import java.net.{InetAddress, InetSocketAddress}
import java.nio.charset.Charset

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
      private val masterAddress = new InetSocketAddress(InetAddress.getLocalHost, Setting.MasterPort)

      val listener = new MessageListener(masterAddress)
      val serverSubscription: Subscription = listener.startServer

      val numSlaves = _numSlaves

      private var remainingSlaves = numSlaves

      private val slaveList = new ArrayBuffer[InetSocketAddress]()
      private val sampleKeys = new ArrayBuffer[Key]()

      def run() = {
        val p = Promise[PartitionTable]()
        listener.replaceHandler(new MessageHandler {
          def handleMessage(message: Message): Future[Unit] = Future {
            message.messageType match {
              case MessageType.Introduce =>
                slaveList += InetSocketAddressSerializer.fromByteArray(message.data)
              case MessageType.SampleData =>
                sampleKeys ++= KeyListSerializer.fromByteArray(message.data)
                remainingSlaves -= 1
                if (remainingSlaves == 0) p.success(createPartitionTable())
              case _ => ()
            }
          }
        })
        p.future
      }

      private def createPartitionTable() = {
        val sortedKeys = sampleKeys.sortBy(key => new String(key, Charset.forName("US-ASCII")))
        val step = sortedKeys.length / numSlaves
        val slaveRanges = slaveList.zipWithIndex.map {
          case (socketAddress, i) => new SlaveRange(socketAddress, sortedKeys(i*step))
        }
        new PartitionTable(Master, masterAddress, slaveRanges.toVector)
      }
    }
  }

  def preparePartitioning(prevState: SamplingState)(partitionTable: PartitionTable): PartitioningState = {
    val _partitionTable = partitionTable
    new PartitioningState {
      val listener = prevState.listener
      val serverSubscription: Subscription = prevState.serverSubscription

      val numSlaves = prevState.numSlaves

      val partitionTable = _partitionTable

      def run() = {
        val p = Promise[Unit]()
        listener.replaceHandler(new MessageHandler {
          def handleMessage(message: Message): Future[Unit] = Future {
            ()
          }
        })
        // TODO: send serialized partition table
        p.future
      }
    }
  }
}