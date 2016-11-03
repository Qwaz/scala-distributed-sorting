package dsorting.transition

import java.net.{InetAddress, InetSocketAddress, URI, URISyntaxException}
import java.util
import java.util.Collections

import com.typesafe.scalalogging.Logger
import dsorting.common.Setting
import dsorting.common.future.Subscription
import dsorting.common.messaging._
import dsorting.common.primitive._
import dsorting.serializer.{InetSocketAddressSerializer, KeyListSerializer, PartitionTableSerializer}
import dsorting.states.slave._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

package object slave {
  val logger = Logger("Slave")

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
    new SamplingState {
      val selfAddress = new InetSocketAddress(InetAddress.getLocalHost.getHostAddress, Setting.SlavePort)
      logger.debug(s"Self Address: $selfAddress")
      logger.debug(s"Master Address: ${slaveStartupInfo.masterAddress}")

      val listener = new MessageListener(selfAddress)
      val serverSubscription: Subscription = listener.startServer

      val channelToMaster = new Channel(Master, slaveStartupInfo.masterAddress)

      val ioDirectoryInfo = slaveStartupInfo.ioDirectoryInfo

      def run() = {
        logger.info("Sampling: run()")

        val p = Promise[PartitionTable]()

        def receivePartitionData(data: Array[Byte]) = {
          val partitionTable = PartitionTableSerializer.fromByteArray(data)
          p.success(partitionTable)
          logger.debug(s"Partition Table received - ${partitionTable.identity}")
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
        val second = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

        Collections.shuffle(util.Arrays.asList(first))
        Collections.shuffle(util.Arrays.asList(second))

        channelToMaster.sendMessage(new Message(
          MessageType.SampleData,
          KeyListSerializer.toByteArray(Key(first) :: Key(second) :: Nil)
        ))
      }

      logger.info("Sampling: Initialized")
    }
  }

  def preparePartitioning(prevState: SamplingState)(partitionTable: PartitionTable): PartitioningState = {
    val _partitionTable = partitionTable
    new PartitioningState {
      val selfAddress = prevState.selfAddress
      val listener = prevState.listener
      val serverSubscription = prevState.serverSubscription

      val channelToMaster = prevState.channelToMaster

      val ioDirectoryInfo = prevState.ioDirectoryInfo

      val partitionTable = _partitionTable
      val channelTable = ChannelTable.fromPartitionTable(partitionTable)

      def run() = {
        logger.info("Partitioning: run()")

        val p = Promise[Unit]()

        def receivePartitionComplete(data: Array[Byte]) = {
          p.success(())
          logger.debug(s"Partitioning complete")
        }

        listener.replaceHandler(new MessageHandler {
          def handleMessage(message: Message): Unit = message.messageType match {
            case MessageType.PartitionComplete => receivePartitionComplete(message.data)
            case _ => ()
          }
        })

        sendOkData()

        p.future
      }

      private def sendOkData() = {
        channelToMaster.sendMessage(Message.withType(MessageType.PartitionOk))
      }
    }
  }
}