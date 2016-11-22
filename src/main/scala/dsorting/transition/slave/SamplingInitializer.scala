package dsorting.transition.slave

import com.typesafe.scalalogging.Logger
import dsorting.Setting
import dsorting.diskio._
import dsorting.messaging._
import dsorting.primitive._
import dsorting.serializer._
import dsorting.states.slave._

import scala.collection.mutable
import scala.concurrent.Promise

object SamplingInitializer {
  def prepareSampling(slaveStartupInfo: SlaveStartupInfo): SamplingState = {
    new FreshState(slaveStartupInfo) with SamplingState {
      val logger = Logger("Slave Sampling")

      def run() = {
        logger.info("start running")

        val p = Promise[PartitionTable]()

        def receivePartitionData(data: Array[Byte]) = {
          val partitionTable = PartitionTableSerializer.fromByteArray(data)
          logger.debug(partitionTable.toString)
          p.success(partitionTable)
        }

        listener.replaceHandler{
          message => message.messageType match {
            case MessageType.PartitionData => receivePartitionData(message.data)
            case _ => ()
          }
        }

        introduce()
        performSampling()

        p.future
      }

      private def introduce() = {
        channelToMaster.sendMessage(new Message(
          MessageType.Introduce,
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
          MessageType.SampleData,
          KeyListSerializer.toByteArray(keys)
        ))
      }

      logger.info("initialized")
    }
  }
}
