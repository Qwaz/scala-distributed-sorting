package dsorting.states.master

import java.net.{InetAddress, InetSocketAddress}

import dsorting.common.Setting
import dsorting.common.primitive._

class SamplingState(val numSlaves: Integer) {
  private val masterAddress = new InetSocketAddress(InetAddress.getLocalHost, Setting.MasterPort)

  val partitionTable = new PartitionTable(Master, masterAddress)
}