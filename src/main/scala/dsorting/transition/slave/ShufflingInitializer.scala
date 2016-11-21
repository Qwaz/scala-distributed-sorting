package dsorting.transition.slave

import com.typesafe.scalalogging.Logger
import dsorting.messaging._
import dsorting.primitive._
import dsorting.states.slave._

import scala.concurrent.Promise

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

        def receiveShufflingStart(data: Array[Byte]) = {
          logger.debug(s"shuffling start")
          p.success(())
        }

        listener.replaceHandler(new MessageHandler {
          def handleMessage(message: Message): Unit = message.messageType match {
            case MessageType.ShufflingStart => receiveShufflingStart(message.data)
            case _ => ()
          }
        })

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
