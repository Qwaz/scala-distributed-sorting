package dsorting.transition.master

import com.typesafe.scalalogging.Logger
import dsorting.messaging._
import dsorting.states.master._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

object ShufflingInitializer {
  def prepareShuffling(prevState: PartitioningState)(unit: Unit): ShufflingState = {
    new TransitionFromConnected(prevState) with ShufflingState {
      val logger = Logger("Master Shuffling")

      private var readyCount = numSlaves
      private var doneCount = numSlaves

      def run(): Future[Unit] = {
        logger.info("start running")

        val p = Promise[Unit]()

        def receiveShufflingReady(data: Array[Byte]): Unit = {
          readyCount -= 1
          logger.debug(s"shuffling ready received: $readyCount remains")
          if (readyCount == 0) {
            channelTable.broadcast(Message.withType(MessageType.ShufflingStart))
          }
        }

        def receiveShufflingDone(data: Array[Byte]): Unit = {
          doneCount -= 1
          logger.debug(s"shuffling done received: $doneCount remains")
          if (doneCount == 0) {
            channelTable.broadcast(Message.withType(MessageType.ShufflingComplete))
            p.success(())
          }
        }

        listener.replaceHandler {
          (message, futurama) => {
            message.messageType match {
              case MessageType.ShufflingReady => futurama.executeAfter("Shuffling Ready")(receiveShufflingReady, message.data)
              case MessageType.ShufflingDone => futurama.executeAfter("Shuffling Done")(receiveShufflingDone, message.data)
              case _ => Future()
            }
          }
        }

        p.future
      }

      logger.info("initialized")
    }
  }
}
