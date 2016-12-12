package dsorting.transition.master

import com.typesafe.scalalogging.Logger
import dsorting.messaging._
import dsorting.states.master._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

object SortingInitializer {
  def prepareSorting(prevState: ShufflingState)(unit: Unit): SortingState = {
    new TransitionFromConnected(prevState) with SortingState {
      val logger = Logger("Master Sorting")

      private var doneCount = numSlaves

      def run(): Future[Unit] = {
        logger.info("start running")

        val p = Promise[Unit]()

        def receiveSortingDone(data: Array[Byte]): Unit = {
          doneCount -= 1
          logger.debug(s"sorting done received: $doneCount remains")
          if (doneCount == 0) {
            channelTable.broadcast(Message.withType(MessageType.SortingComplete))
            p.success(())
          }
        }

        listener.replaceHandler {
          (message, futurama) => {
            message.messageType match {
              case MessageType.SortingDone => futurama.executeAfter("Sorting Done")(receiveSortingDone, message.data)
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
