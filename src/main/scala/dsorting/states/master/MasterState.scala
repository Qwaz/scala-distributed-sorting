package dsorting.states.master

import dsorting.common.future.Subscription
import dsorting.common.messaging._
import dsorting.common.primitive._

trait MasterState[T] extends State[T] {
  val listener: MessageListener
  val serverSubscription: Subscription

  val numSlaves: Integer
}

trait SamplingState extends MasterState[PartitionTable]

trait PartitioningState extends MasterState[Unit] {
  val partitionTable: PartitionTable
  val channelTable: ChannelTable
}