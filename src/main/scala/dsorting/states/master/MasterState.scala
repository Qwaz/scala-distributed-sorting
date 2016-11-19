package dsorting.states.master

import java.net.{InetAddress, InetSocketAddress}

import com.typesafe.scalalogging.Logger
import dsorting.Setting
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


class FreshState(port: Integer) {
  private val masterAddress = new InetSocketAddress(InetAddress.getLocalHost.getHostAddress, Setting.MasterPort)

  val listener = new MessageListener(masterAddress)
  val serverSubscription: Subscription = listener.startServer

  private val logger = Logger("Fresh Master State")
  logger.debug(s"master address: $masterAddress")
}

class TransitionFrom[T](prevState: MasterState[T]) {
  val listener = prevState.listener
  val serverSubscription: Subscription = prevState.serverSubscription

  val numSlaves = prevState.numSlaves
}