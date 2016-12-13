package dsorting.states.master

import com.typesafe.scalalogging.Logger
import dsorting.messaging._
import dsorting.states.ConnectedWorkers

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

trait SortingState extends MasterState[Seq[String]] with ConnectedWorkers

object SortingStateFactory {
  def apply(prevState: ShufflingState)(unit: Unit): SortingState = {
    new TransitionFromConnected(prevState) with SortingState {
      val logger = Logger("Master Sorting")

      private var doneCount = numSlaves

      def run(): Future[Seq[String]] = {
        logger.info("start running")

        val p = Promise[Seq[String]]()

        def receiveSortingDone(data: Array[Byte]): Unit = {
          doneCount -= 1
          logger.debug(s"sorting done received: $doneCount remains")
          if (doneCount == 0) {
            channelTable.broadcast(Message.withType(MessageType.SortingComplete))
            p.success(partitionTable.slaveRanges.map {
              slaveRange => slaveRange.slave.getAddress.toString.substring(1)
            })
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

        channelTable.broadcast(Message.withType(MessageType.ShufflingComplete))

        p.future
      }

      logger.info("initialized")
    }
  }
}
