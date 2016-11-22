package dsorting.transition.slave

import com.typesafe.scalalogging.Logger
import dsorting.Setting
import dsorting.diskio.{Directory, FileEntryReader}
import dsorting.messaging._
import dsorting.primitive._
import dsorting.serializer.EntrySerializer
import dsorting.states.slave._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise, blocking}
import scala.util.{Failure, Success}

object ShufflingInitializer {
  def prepareShuffling(prevState: SamplingState)(partitionTable: PartitionTable): ShufflingState = {
    val _partitionTable = partitionTable
    new TransitionFrom(prevState) with ShufflingState {
      val logger = Logger("Slave Shuffling")

      val partitionTable = _partitionTable
      val channelTable = ChannelTable.fromPartitionTable(partitionTable)

      def run() = {
        logger.info("start running")

        val p = Promise[Unit]()

        def receiveShufflingStart() = {
          logger.debug(s"shuffling start")
          performShuffling() onComplete {
            case Success(_) => channelToMaster.sendMessage(Message.withType(MessageType.ShufflingDone))
            case Failure(e) => p.failure(e)
          }
        }

        def receiveShufflingComplete() = {
          logger.debug(s"shuffling complete")
          p.success(())
        }

        def receiveShuffle(data: Array[Byte]) = {
          val entry = EntrySerializer.fromByteArray(data)
          //logger.debug(entry.toString)
        }

        listener.replaceHandler {
          message => message.messageType match {
            case MessageType.ShufflingStart => receiveShufflingStart()
            case MessageType.ShufflingComplete => receiveShufflingComplete()
            case MessageType.Shuffle => receiveShuffle(message.data)
            case _ => ()
          }
        }

        sendReady()

        p.future
      }

      private def sendReady() = {
        channelToMaster.sendMessage(Message.withType(MessageType.ShufflingReady))
      }

      private def performShuffling() = Future {
        blocking {
          for (directoryName <- ioDirectoryInfo.inputDirectories) {
            val directory = Directory(directoryName)
            val inputFiles = directory.listFilesWithPrefix(Setting.InputFilePrefix)
            for (inputFile <- inputFiles) {
              val reader = new FileEntryReader(inputFile)
              while (reader.hasNext) {
                val entry = reader.next()
                val slaveIndex = partitionTable.findSlaveIndexForKey(entry.key)
                channelTable(slaveIndex).sendMessage(new Message(MessageType.Shuffle, EntrySerializer.toByteArray(entry)))
              }
              reader.close()
            }
          }
        }
      }

      logger.info("initialized")
    }
  }
}
