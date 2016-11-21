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

      private var remainingSlaves = numSlaves

      def run() = {
        logger.info("start running")

        val p = Promise[Unit]()

        def receiveShufflingReady(data: Array[Byte]) = {
          remainingSlaves -= 1
          logger.debug(s"shuffling ready received: $remainingSlaves remains")
          if (remainingSlaves == 0) {
            channelTable.broadcast(Message.withType(MessageType.ShufflingStart))
            p.success(())
          }
        }

        listener.replaceHandler {
          message => message.messageType match {
            case MessageType.ShufflingReady => receiveShufflingReady(message.data)
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
