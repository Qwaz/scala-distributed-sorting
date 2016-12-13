package dsorting.states.slave

import java.io.{File, FileInputStream, FileOutputStream}

import com.typesafe.scalalogging.Logger
import dsorting.Setting
import dsorting.diskio._
import dsorting.messaging._
import dsorting.primitive._
import dsorting.serializer.ShufflingDataSerializer
import dsorting.states.ConnectedWorkers

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

trait ShufflingState extends SlaveState[File] with ConnectedWorkers

class ShufflingData(val fromSlave: Int, val data: Array[Byte])

object ShufflingStateFactory {
  def apply(prevState: PartitioningState)(entryInputStreams: IndexedSeq[FileInputStream]): ShufflingState = {
    new TransitionFromConnected(prevState) with ShufflingState {
      val logger = Logger("Slave Shuffling")

      private val ShufflingPrefix = "shuffling"
      private val outputDirectory = Directory(ioDirectoryInfo.outputDirectory)
      outputDirectory.deleteFilesWithPrefix(ShufflingPrefix)
      private val outputFile = outputDirectory.createTemporaryFileWithPrefix(ShufflingPrefix)
      private val entryOutputStream = new FileOutputStream(outputFile)

      private var receiveDoneCount = partitionTable.slaveRanges.length
      private val mySlaveIndex = {
        partitionTable.identity match {
          case Master => -1
          case Slave(i) => i
        }
      }

      def run(): Future[File] = {
        logger.info("start running")

        val p = Promise[File]()

        def receiveShufflingStart(data: Array[Byte]): Unit = {
          logger.debug("shuffling start")
          channelTable.broadcast(new Message(
            MessageType.ShufflingDataNext,
            Util.intToByte(mySlaveIndex)
          ))
        }

        def receiveShufflingComplete(data: Array[Byte]): Unit = {
          logger.debug("shuffling complete")
          p.success(outputFile)
        }

        def receiveShufflingData(data: Array[Byte]): Unit = {
          val shufflingData = ShufflingDataSerializer.fromByteArray(data)

          entryOutputStream.write(shufflingData.data)
          entryOutputStream.flush()

          channelTable(shufflingData.fromSlave).sendMessage(new Message(
            MessageType.ShufflingDataNext,
            Util.intToByte(mySlaveIndex)
          ))
        }

        def receiveShufflingDataNext(data: Array[Byte]): Unit = {
          val fromIndex = Util.byteToInt(data)
          val currentStream = entryInputStreams(fromIndex)

          if (currentStream.available > 0) {
            val buf = new Array[Byte](Math.min(Setting.ShufflingChunkSize, currentStream.available))
            currentStream.read(buf)
            channelTable(fromIndex).sendMessage(new Message(
              MessageType.ShufflingData,
              ShufflingDataSerializer.toByteArray(new ShufflingData(mySlaveIndex, buf))
            ))
          } else {
            currentStream.close()
            channelTable(fromIndex).sendMessage(Message.withType(MessageType.ShufflingDataDone))
          }
        }

        def receiveShufflingDataDone(data: Array[Byte]): Unit = {
          receiveDoneCount -= 1
          logger.debug(s"Shuffling Data Done - remaining $receiveDoneCount")
          if (receiveDoneCount == 0) {
            entryOutputStream.close()
            channelToMaster.sendMessage(Message.withType(MessageType.ShufflingDone))
          }
        }

        listener.replaceHandler {
          (message, futurama) => {
            message.messageType match {
              case MessageType.ShufflingStart => futurama.executeImmediately(receiveShufflingStart, message.data)
              case MessageType.ShufflingComplete => futurama.executeImmediately(receiveShufflingComplete, message.data)
              case MessageType.ShufflingData => futurama.executeAfter("Shuffling Data")(receiveShufflingData, message.data)
              case MessageType.ShufflingDataNext => futurama.executeImmediately(receiveShufflingDataNext, message.data)
              case MessageType.ShufflingDataDone => futurama.executeAfter("Shuffling Data")(receiveShufflingDataDone, message.data)
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
