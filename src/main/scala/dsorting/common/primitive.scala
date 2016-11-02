package dsorting.common

import java.net.InetSocketAddress

import scala.concurrent.Future

package object primitive {
  type Key = Array[Byte]
  type Value = Array[Byte]

  class IODirectoryInfo(val inputFiles: List[String], val outputDirectory: String) {
    require(inputFiles.nonEmpty)
  }

  class SlaveStartupInfo(val masterAddress: InetSocketAddress, val ioDirectoryInfo: IODirectoryInfo)

  case class Entity(key: Key, value: Value)

  abstract class Identity
  case object Master extends Identity
  case class Slave(index: Integer) extends Identity

  class SlaveRange(val slave: InetSocketAddress, startKey: Key)

  class PartitionTable(val identity: Identity, val masterAddress: InetSocketAddress, val slaveRanges: IndexedSeq[SlaveRange])

  trait State[T] {
    def run(): Future[T]
  }
}
