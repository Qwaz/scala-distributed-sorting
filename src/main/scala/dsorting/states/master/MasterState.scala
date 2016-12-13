package dsorting.states.master

import java.net.{InetAddress, InetSocketAddress}

import com.typesafe.scalalogging.Logger
import dsorting.Setting
import dsorting.future.Subscription
import dsorting.messaging._
import dsorting.primitive._
import dsorting.states._


trait MasterState[T] extends State[T] {
  val numSlaves: Int
}


class FreshState(port: Int) {
  private val masterAddress = new InetSocketAddress(InetAddress.getLocalHost.getHostAddress, Setting.MasterPort)

  val listener = new MessageListener(masterAddress)
  val listenerSubscription: Subscription = listener.start()

  private val logger = Logger("Fresh Master State")
  logger.debug(s"master address: $masterAddress")
}

class TransitionFrom[T](prevState: MasterState[T]) {
  val listener = prevState.listener
  val listenerSubscription: Subscription = prevState.listenerSubscription

  val numSlaves = prevState.numSlaves
}

class TransitionFromConnected[T](prevState: MasterState[T] with ConnectedWorkers) extends TransitionFrom[T](prevState) {
  val partitionTable: PartitionTable = prevState.partitionTable
  val channelTable: ChannelTable = prevState.channelTable
}