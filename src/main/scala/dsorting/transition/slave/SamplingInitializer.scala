package dsorting.transition.slave

import com.typesafe.scalalogging.Logger
import dsorting.Setting
import dsorting.diskio._
import dsorting.messaging._
import dsorting.primitive._
import dsorting.serializer._
import dsorting.states.slave._

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

object SamplingInitializer {
  def prepareSampling(slaveStartupInfo: SlaveStartupInfo): SamplingState = {
    new FreshState(slaveStartupInfo) with SamplingState {
      val logger = Logger("Slave Sampling")

      def run(): Future[PartitionTable] = {
        logger.info("start running")

        val p = Promise[PartitionTable]()

        def receivePartitionData(data: Array[Byte]): Unit = {
          val partitionTable = PartitionTableSerializer.fromByteArray(data)
          logger.debug(partitionTable.toString)
          p.success(partitionTable)
        }

        listener.replaceHandler{
          (message, futurama) => {
            message.messageType match {
              case MessageType.SamplingPartitionTable => futurama.executeImmediately(receivePartitionData, message.data)
              case _ => Future()
            }
          }
        }

        introduce()
        performSampling()

        p.future
      }

      private def introduce() = {
        channelToMaster.sendMessage(new Message(
          MessageType.SamplingIntroduce,
          InetSocketAddressSerializer.toByteArray(selfAddress)
        ))
      }

      private def performSampling() = {
        val keys = mutable.MutableList[Key]()
        for (directoryName <- ioDirectoryInfo.inputDirectories) {
          val directory = Directory(directoryName)
          for (inputFile <- directory.listFilesWithPrefix(Setting.InputFilePrefix)) {
            val reader = new FileEntryReader(inputFile)

            var remain = Setting.NumSamples
            while (remain > 0 && reader.hasNext) {
              val entry = reader.next()
              keys += entry.key
              remain -= 1
            }

            reader.close()
          }
        }

        channelToMaster.sendMessage(new Message(
          MessageType.SamplingSamples,
          KeyListSerializer.toByteArray(keys)
        ))
      }

      logger.info("initialized")
    }
  }
}
