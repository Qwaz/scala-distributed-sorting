package dsorting.common

import java.net.InetSocketAddress

import scala.concurrent.Future

package object primitive {
  class Key(val bytes: Array[Byte])
  class Value(val bytes: Array[Byte])

  object Key {
    def apply(bytes: Array[Byte]) = {
      require(bytes.length == 10)
      new Key(bytes)
    }
  }

  object Value {
    def apply(bytes: Array[Byte]) = {
      require(bytes.length == 90)
      new Key(bytes)
    }
  }

  def emptyKeyBuffer = new Array[Byte](10)

  class IODirectoryInfo(val inputFiles: List[String], val outputDirectory: String) {
    require(inputFiles.nonEmpty)
  }

  class SlaveStartupInfo(val masterAddress: InetSocketAddress, val ioDirectoryInfo: IODirectoryInfo)

  case class Entity(key: Key, value: Value)

  abstract class Identity
  case object Master extends Identity
  case class Slave(index: Integer) extends Identity

  class SlaveRange(val slave: InetSocketAddress, val startKey: Key)

  class PartitionTable(val identity: Identity, val slaveRanges: IndexedSeq[SlaveRange])

  trait State[T] {
    def run(): Future[T]
  }
}
