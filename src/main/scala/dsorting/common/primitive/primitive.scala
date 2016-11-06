package dsorting.common

import java.net.InetSocketAddress
import javax.xml.bind.DatatypeConverter

import scala.concurrent.Future

package object primitive {
  class Key(val bytes: Array[Byte]) {
    override def toString: String = DatatypeConverter.printHexBinary(bytes)
  }

  class Value(val bytes: Array[Byte]) {
    override def toString: String = DatatypeConverter.printHexBinary(bytes)
  }

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
  case object Master extends Identity {
    override def toString: String = "Master"
  }
  case class Slave(index: Integer) extends Identity {
    override def toString: String = s"Slave $index"
  }

  class SlaveRange(val slave: InetSocketAddress, val startKey: Key) {
    override def toString: String = {
      s"Address: $slave / startKey: $startKey"
    }
  }

  class PartitionTable(val identity: Identity, val slaveRanges: IndexedSeq[SlaveRange]) {
    override def toString: String = {
      var str = s"PartitionTable of $identity"
        for (slaveRange <- slaveRanges) {
        str = str + s"\n $slaveRange"
      }
      str
    }
  }

  trait State[T] {
    def run(): Future[T]
  }
}
