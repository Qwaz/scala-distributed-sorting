package dsorting.states.slave

import java.net.InetSocketAddress

import dsorting.future.Subscription
import dsorting.messaging._
import dsorting.primitive._

trait SlaveState[T] extends State[T] {
  val selfAddress: InetSocketAddress

  val listener: MessageListener
  val serverSubscription: Subscription

  val channelToMaster: Channel

  val ioDirectoryInfo: IODirectoryInfo
}

trait SamplingState extends SlaveState[PartitionTable]

trait ShufflingState extends SlaveState[Unit] {
  val partitionTable: PartitionTable
  val channelTable: ChannelTable
}