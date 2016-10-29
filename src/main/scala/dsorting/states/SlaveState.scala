package dsorting.states.slave

import dsorting.common.primitive._

class SamplingState(slaveStartupInfo: SlaveStartupInfo) {
  val partitionTable = new PartitionTable(UnknownSlave, slaveStartupInfo.masterAddress)
  val ioDirectoryInfo = slaveStartupInfo.ioDirectoryInfo
}