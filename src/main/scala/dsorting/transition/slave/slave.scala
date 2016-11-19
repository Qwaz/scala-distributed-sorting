package dsorting.transition

import java.net.{InetAddress, InetSocketAddress, URI, URISyntaxException}
import java.util
import java.util.Collections

import com.typesafe.scalalogging.Logger
import dsorting.Setting
import dsorting.future.Subscription
import dsorting.messaging._
import dsorting.primitive._
import dsorting.serializer._
import dsorting.states.slave._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

package object slave {
  def parseArgument(args: Array[String]): Future[SlaveStartupInfo] = Future {
    def inetSocketAddressFromString(str: String): InetSocketAddress = {
      /*
      http://stackoverflow.com/questions/2345063/java-common-way-to-validate-and-convert-hostport-to-inetsocketaddress
      Use custom scheme to parse host:port from a string
       */
      val uri = new URI("my://" + str)
      val host = uri.getHost
      val port = uri.getPort

      if (host == null || port == -1)
        throw new URISyntaxException(uri.toString, "URI must have host and port parts")
      else
        new InetSocketAddress(host, port)
    }

    val indexI = args.indexOf("-I")
    val indexO = args.indexOf("-O", indexI)
    if (indexI != 1 || indexO != args.length - 2)
      throw new IllegalArgumentException("Usage: master_ip:port -I <input_directory 1> <input_directory 2> ... -O output_directory")
    else
      new SlaveStartupInfo(inetSocketAddressFromString(args(0)), new IODirectoryInfo(args.slice(indexI+1, indexO).toList, args(indexO+1)))
  }

  def prepareSampling(slaveStartupInfo: SlaveStartupInfo): SamplingState = {
    new FreshState(slaveStartupInfo) with SamplingState {
      val logger = Logger("Slave Sampling")

      def run() = {
        logger.info("start running")

        val p = Promise[PartitionTable]()

        def receivePartitionData(data: Array[Byte]) = {
          val partitionTable = PartitionTableSerializer.fromByteArray(data)
          p.success(partitionTable)
          logger.debug(partitionTable.toString)
        }

        listener.replaceHandler(new MessageHandler {
          def handleMessage(message: Message): Unit = {
            message.messageType match {
              case MessageType.PartitionData => receivePartitionData(message.data)
            }
          }
        })

        introduce()
        performSampling()

        p.future
      }

      private def introduce() = {
        channelToMaster.sendMessage(new Message(
          MessageType.Introduce,
          InetSocketAddressSerializer.toByteArray(selfAddress)
        ))
      }

      private def performSampling() = {
        // TODO: change to real sampling
        val first = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
        val second = Array[Byte](9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

        Collections.shuffle(util.Arrays.asList(first))
        Collections.shuffle(util.Arrays.asList(second))

        channelToMaster.sendMessage(new Message(
          MessageType.SampleData,
          KeyListSerializer.toByteArray(Key(first) :: Key(second) :: Nil)
        ))
      }

      logger.info("initialized")
    }
  }

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