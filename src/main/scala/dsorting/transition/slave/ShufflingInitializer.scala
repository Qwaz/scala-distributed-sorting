package dsorting.transition.slave

import com.typesafe.scalalogging.Logger
import dsorting.diskio._
import dsorting.messaging._
import dsorting.states.slave._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

object ShufflingInitializer {
  def prepareShuffling(prevState: PartitioningState)(entryReaders: IndexedSeq[EntryReader]): ShufflingState = {
    new TransitionFromConnected(prevState) with ShufflingState {
      val logger = Logger("Slave Shuffling")

      private val ShufflingPrefix = "shuffling"
      private val outputDirectory = Directory(ioDirectoryInfo.outputDirectory)
      outputDirectory.deleteFilesWithPrefix(ShufflingPrefix)
      private val entryWriter = new FileEntryWriter(outputDirectory.createTemporaryFileWithPrefix(ShufflingPrefix))

      def run(): Future[Unit] = {
        logger.info("start running")

        val p = Promise[Unit]()

        def receiveShufflingStart(data: Array[Byte]): Unit = {
          logger.debug("shuffling start")
          // TODO: replace with shuffling start code
          channelToMaster.sendMessage(Message.withType(MessageType.ShufflingDone))
        }

        def receiveShufflingComplete(data: Array[Byte]): Unit = {
          logger.debug("shuffling complete")
          p.success(())
        }

        listener.replaceHandler {
          (message, futurama) => {
            message.messageType match {
              // TODO: handle shuffling data
              case MessageType.ShufflingStart => futurama.executeImmediately(receiveShufflingStart, message.data)
              case MessageType.ShufflingComplete => futurama.executeImmediately(receiveShufflingComplete, message.data)
              case _ => Future()
            }
          }
        }

        sendReady()

        p.future
      }

      private def sendReady() = {
        channelToMaster.sendMessage(Message.withType(MessageType.ShufflingReady))
      }

      logger.info("initialized")
    }
  }
}
