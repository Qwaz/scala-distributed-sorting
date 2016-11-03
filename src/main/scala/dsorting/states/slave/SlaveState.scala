package dsorting.states.slave

import java.net.InetSocketAddress

import dsorting.common.future.Subscription
import dsorting.common.messaging._
import dsorting.common.primitive._

trait SlaveState[T] extends State[T] {
  val selfAddress: InetSocketAddress

  val listener: MessageListener
  val serverSubscription: Subscription

  val channelToMaster: Channel

  val ioDirectoryInfo: IODirectoryInfo
}

trait SamplingState extends SlaveState[PartitionTable]

trait PartitioningState extends SlaveState[Unit] {
  val partitionTable: PartitionTable
  val channelTable: ChannelTable
}