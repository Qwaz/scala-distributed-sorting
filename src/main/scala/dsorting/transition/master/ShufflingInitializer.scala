package dsorting.transition.master

import com.typesafe.scalalogging.Logger
import dsorting.messaging._
import dsorting.primitive._
import dsorting.serializer._
import dsorting.states.master._

import scala.concurrent.Promise

object ShufflingInitializer {
  def prepareShuffling(prevState: SamplingState)(partitionTable: PartitionTable): ShufflingState = {
    val _partitionTable = partitionTable
    new TransitionFrom(prevState) with ShufflingState {
      val logger = Logger("Master Shuffling")

      val partitionTable = _partitionTable
      logger.debug(partitionTable.toString)
      val channelTable = ChannelTable.fromPartitionTable(partitionTable)

      private var readyCount = numSlaves
      private var doneCount = numSlaves

      def run() = {
        logger.info("start running")

        val p = Promise[Unit]()

        def receiveShufflingReady() = {
          readyCount -= 1
          logger.debug(s"shuffling ready received: $readyCount remains")
          if (readyCount == 0) {
            channelTable.broadcast(Message.withType(MessageType.ShufflingStart))
          }
        }

        def receiveShufflingDone() = {
          doneCount -= 1
          logger.debug(s"shuffling done received: $doneCount remains")
          if (doneCount == 0) {
            channelTable.broadcast(Message.withType(MessageType.ShufflingComplete))
            p.success(())
          }
        }

        listener.replaceHandler {
          message => message.messageType match {
            case MessageType.ShufflingReady => receiveShufflingReady()
            case MessageType.ShufflingDone => receiveShufflingDone()
            case _ => ()
          }
        }

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
