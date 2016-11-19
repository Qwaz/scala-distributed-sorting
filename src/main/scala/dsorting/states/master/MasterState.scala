package dsorting.states.master

import dsorting.future.Subscription
import dsorting.messaging._
import dsorting.primitive._

trait MasterState[T] extends State[T] {
  val listener: MessageListener
  val serverSubscription: Subscription

  val numSlaves: Integer
}

trait SamplingState extends MasterState[PartitionTable]

trait ShufflingState extends MasterState[Unit] {
  val partitionTable: PartitionTable
  val channelTable: ChannelTable
}