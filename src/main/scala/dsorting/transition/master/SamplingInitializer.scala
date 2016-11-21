package dsorting.transition.master

import java.net.InetSocketAddress
import java.nio.charset.Charset

import com.typesafe.scalalogging.Logger
import dsorting.Setting
import dsorting.messaging._
import dsorting.primitive._
import dsorting.serializer._
import dsorting.states.master._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Promise

object SamplingInitializer {
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

        listener.replaceHandler {
          message => message.messageType match {
            case MessageType.Introduce => receiveIntroduce(message.data)
            case MessageType.SampleData => receiveSampleData(message.data)
            case _ => ()
          }
        }

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
}
