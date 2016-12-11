package dsorting.transition.master

import com.typesafe.scalalogging.Logger
import dsorting.messaging._
import dsorting.primitive._
import dsorting.serializer.PartitionTableSerializer
import dsorting.states.master._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

object PartitioningInitializer {
  def preparePartitioning(prevState: SamplingState)(partitionTable: PartitionTable): PartitioningState = {
    val _partitionTable = partitionTable
    new TransitionFrom(prevState) with PartitioningState {
      val logger = Logger("Master Partitioning")

      val partitionTable: PartitionTable = _partitionTable
      logger.debug(partitionTable.toString)
      val channelTable: ChannelTable = ChannelTable.fromPartitionTable(partitionTable)

      private var doneCount = numSlaves

      def run(): Future[Unit] = {
        logger.info("start running")

        val p = Promise[Unit]()

        def receivePartitionDone(data: Array[Byte]): Unit = {
          doneCount -= 1
          logger.debug(s"partition done received: $doneCount remains")
          if (doneCount == 0) {
            channelTable.broadcast(Message.withType(MessageType.PartitioningComplete))
            p.success(())
          }
        }

        listener.replaceHandler {
          (message, futurama) => {
            message.messageType match {
              case MessageType.PartitioningDone => futurama.executeAfter("Partitioning Done")(receivePartitionDone, message.data)
              case _ => Future()
            }
          }
        }

        broadcastPartitionTable()

        p.future
      }

      private def broadcastPartitionTable() = {
        channelTable.channels.zipWithIndex.foreach {
          case (channel, i) =>
            channel.sendMessage(new Message(
              MessageType.SamplingPartitionTable,
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
