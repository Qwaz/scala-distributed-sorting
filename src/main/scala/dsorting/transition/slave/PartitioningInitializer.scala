package dsorting.transition.slave

import java.io.FileInputStream

import com.typesafe.scalalogging.Logger
import dsorting.Setting
import dsorting.diskio._
import dsorting.future._
import dsorting.messaging._
import dsorting.primitive._
import dsorting.states.slave._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

object PartitioningInitializer {
  def preparePartitioning(prevState: SamplingState)(partitionTable: PartitionTable): PartitioningState = {
    val _partitionTable = partitionTable
    new TransitionFrom(prevState) with PartitioningState {
      val logger = Logger("Slave Partitioning")

      val partitionTable: PartitionTable = _partitionTable
      val channelTable: ChannelTable = ChannelTable.fromPartitionTable(partitionTable)

      private val PartitioningPrefix = "partitioning"
      private val outputDirectory = Directory(ioDirectoryInfo.outputDirectory)
      outputDirectory.deleteFilesWithPrefix(PartitioningPrefix)
      private val entryWriters = partitionTable.slaveRanges.map {
        _ => new FileEntryWriter(outputDirectory.createTemporaryFileWithPrefix(PartitioningPrefix))
      }
      private val entryInputStreams =
        entryWriters map {
          entryWriter => new FileInputStream(entryWriter.file)
        }

      def run(): Future[IndexedSeq[FileInputStream]] = {
        logger.info("start running")

        val p = Promise[IndexedSeq[FileInputStream]]()

        def receivePartitionComplete(data: Array[Byte]): Unit = {
          logger.debug("partitioning complete")
          p.success(entryInputStreams)
        }

        listener.replaceHandler {
          (message, futurama) => {
            message.messageType match {
              case MessageType.PartitioningComplete => futurama.executeImmediately(receivePartitionComplete, message.data)
              case _ => Future()
            }
          }
        }

        performPartitioning().logError("Partitioning Error")

        p.future
      }

      private def performPartitioning(): Future[Unit] = Future {
        for (directoryName <- ioDirectoryInfo.inputDirectories) {
          val directory = Directory(directoryName)
          val inputFiles = directory.listFilesWithPrefix(Setting.InputFilePrefix)
          for (inputFile <- inputFiles) {
            val reader = new FileEntryReader(inputFile)
            while (reader.hasNext) {
              val entry = reader.next()
              val slaveIndex = partitionTable.findSlaveIndexForKey(entry.key)
              entryWriters(slaveIndex).writeEntry(entry)
            }
            reader.close()
          }
        }

        entryWriters foreach {
          entryWriter => entryWriter.close()
        }

        channelToMaster.sendMessage(Message.withType(MessageType.PartitioningDone))
      }

      logger.info("initialized")
    }
  }
}
