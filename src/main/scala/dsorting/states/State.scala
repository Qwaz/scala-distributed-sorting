package dsorting.states

import dsorting.future.Subscription
import dsorting.messaging.{ChannelTable, MessageListener}
import dsorting.primitive.PartitionTable

import scala.concurrent.Future

trait State[T] {
  val listener: MessageListener
  val listenerSubscription: Subscription

  def run(): Future[T]
}

trait ConnectedWorkers {
  val partitionTable: PartitionTable
  val channelTable: ChannelTable
}