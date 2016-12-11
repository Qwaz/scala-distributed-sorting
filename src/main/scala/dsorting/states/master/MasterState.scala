package dsorting.states.master

import java.net.{InetAddress, InetSocketAddress}

import com.typesafe.scalalogging.Logger
import dsorting.Setting
import dsorting.future.Subscription
import dsorting.messaging._
import dsorting.primitive._

trait MasterState[T] extends State[T] {
  val listener: MessageListener
  val listenerSubscription: Subscription

  val numSlaves: Int
}


trait SamplingState extends MasterState[PartitionTable]

trait ShufflingState extends MasterState[Unit] {
  val partitionTable: PartitionTable
  val channelTable: ChannelTable
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