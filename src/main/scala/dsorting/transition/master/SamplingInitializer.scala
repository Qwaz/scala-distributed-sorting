package dsorting.transition.master

import java.net.InetSocketAddress

import com.typesafe.scalalogging.Logger
import dsorting.Setting
import dsorting.messaging._
import dsorting.primitive._
import dsorting.serializer._
import dsorting.states.master._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

object SamplingInitializer {
  def prepareSampling(numSlaves: Int): SamplingState = {
    val _numSlaves = numSlaves
    new FreshState(Setting.MasterPort) with SamplingState {
      val logger = Logger("Master Sampling")

      val numSlaves: Int = _numSlaves
      logger.debug(s"numSlaves $numSlaves")

      private var remainingSlaves = numSlaves

      private val slaveAddresses = ArrayBuffer[InetSocketAddress]()
      private val sampleKeys = ArrayBuffer[Key](Key(Array[Byte](0, 0, 0, 0, 0, 0, 0, 0, 0, 0)))

      def run(): Future[PartitionTable] = {
        logger.info("start running")

        val p = Promise[PartitionTable]()

        def receiveIntroduce(data: Array[Byte]): Unit = {
          val slaveAddress = InetSocketAddressSerializer.fromByteArray(data)
          slaveAddresses += slaveAddress
          logger.debug(s"introduce data received from $slaveAddress")
        }

        def receiveSampleData(data: Array[Byte]): Unit = {
          sampleKeys ++= KeyListSerializer.fromByteArray(data)
          remainingSlaves -= 1
          logger.debug(s"sample data received: $remainingSlaves remains")
          if (remainingSlaves == 0) p.success(createPartitionTable())
        }

        listener.replaceHandler {
          (message, futurama) => {
            message.messageType match {
              case MessageType.SamplingIntroduce => futurama.executeAfter("Sampling Introduce")(receiveIntroduce, message.data)
              case MessageType.SamplingSamples => futurama.executeAfter("Sampling Samples")(receiveSampleData, message.data)
              case _ => Future()
            }
          }
        }

        p.future
      }

      private def createPartitionTable() = {
        val sortedKeys = sampleKeys.sortWith(_ <= _)
        val step = sortedKeys.length / numSlaves
        val slaveRanges = slaveAddresses.zipWithIndex.map {
          case (socketAddress, i) => new SlaveRange(socketAddress, sortedKeys(i*step))
        }
        new PartitionTable(Master, slaveRanges.toVector)
      }

      logger.info("initialized")
    }
  }
}
