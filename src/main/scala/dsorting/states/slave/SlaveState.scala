package dsorting.states.slave

import java.net.{InetAddress, InetSocketAddress}

import com.typesafe.scalalogging.Logger
import dsorting.Setting
import dsorting.future.Subscription
import dsorting.messaging._
import dsorting.primitive._
import dsorting.states._


class IODirectoryInfo(val inputDirectories: List[String], val outputDirectory: String) {
  require(inputDirectories.nonEmpty)
}

class SlaveStartupInfo(val masterAddress: InetSocketAddress, val ioDirectoryInfo: IODirectoryInfo)


trait SlaveState[T] extends State[T] {
  val selfAddress: InetSocketAddress
  val channelToMaster: Channel
  val ioDirectoryInfo: IODirectoryInfo
}


class FreshState(slaveStartupInfo: SlaveStartupInfo) {
  val selfAddress = new InetSocketAddress(InetAddress.getLocalHost.getHostAddress, Setting.SlavePort)

  val listener = new MessageListener(selfAddress)
  val listenerSubscription: Subscription = listener.start()

  val channelToMaster = new Channel(Master, slaveStartupInfo.masterAddress)

  val ioDirectoryInfo = slaveStartupInfo.ioDirectoryInfo

  private val logger = Logger("Fresh Slave State")
  logger.debug(s"self address: $selfAddress")
  logger.debug(s"master address: ${slaveStartupInfo.masterAddress}")
}

class TransitionFrom[T](prevState: SlaveState[T]) {
  val selfAddress = prevState.selfAddress

  val listener = prevState.listener
  val listenerSubscription = prevState.listenerSubscription

  val channelToMaster = prevState.channelToMaster

  val ioDirectoryInfo = prevState.ioDirectoryInfo
}

class TransitionFromConnected[T](prevState: SlaveState[T] with ConnectedWorkers) extends TransitionFrom[T](prevState) {
  val partitionTable: PartitionTable = prevState.partitionTable
  val channelTable: ChannelTable = prevState.channelTable
}