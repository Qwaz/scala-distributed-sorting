package dsorting.states.slave

import java.io.{File, FileInputStream}
import java.net.{InetAddress, InetSocketAddress}

import com.typesafe.scalalogging.Logger
import dsorting.Setting
import dsorting.future.Subscription
import dsorting.messaging._
import dsorting.primitive._

trait SlaveState[T] extends State[T] {
  val selfAddress: InetSocketAddress

  val listener: MessageListener
  val listenerSubscription: Subscription

  val channelToMaster: Channel

  val ioDirectoryInfo: IODirectoryInfo
}


trait SamplingState extends SlaveState[PartitionTable]

trait PartitioningState extends SlaveState[IndexedSeq[FileInputStream]] with ConnectedWorkers

trait ShufflingState extends SlaveState[File] with ConnectedWorkers

trait SortingState extends SlaveState[Unit] with ConnectedWorkers


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