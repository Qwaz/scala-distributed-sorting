package dsorting.primitive

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import javax.xml.bind.DatatypeConverter

import dsorting.Setting
import dsorting.messaging.ChannelTable

import scala.concurrent.Future

object Util {
  def intToByte(int: Int) = {
    val buf = ByteBuffer.allocate(4)
    buf.putInt(int)
    buf.array()
  }

  def byteToInt(data: Array[Byte]) = {
    require(data.length == 4)
    val buf = ByteBuffer.allocate(4)
    buf.put(data)
    buf.flip()
    buf.getInt()
  }
}

class Key(val bytes: Array[Byte]) {
  override def toString: String = DatatypeConverter.printHexBinary(bytes)

  def <= (key: Key) = {
    var index = 0
    while (index < Setting.KeySize && bytes(index) == key.bytes(index)) index += 1
    if (index == Setting.KeySize || (bytes(index) & 0xFF) <= (key.bytes(index) & 0xFF)) true
    else false
  }
}

class Value(val bytes: Array[Byte]) {
  override def toString: String = DatatypeConverter.printHexBinary(bytes)
}

object Key {
  def apply(bytes: Array[Byte]) = {
    require(bytes.length == Setting.KeySize)
    new Key(bytes)
  }
}

object Value {
  def apply(bytes: Array[Byte]) = {
    require(bytes.length == Setting.ValueSize)
    new Value(bytes)
  }
}

case class Entry(key: Key, value: Value) {
  override def toString: String = {
    s"Entry ($key $value)"
  }

  def <= (entry: Entry) = {
    key <= entry.key
  }
}

object BufferFactory {
  val size0 = new Array[Byte](0)
  def emptyKeyBuffer() = new Array[Byte](Setting.KeySize)
  def emptyValueBuffer() = new Array[Byte](Setting.ValueSize)
}

class IODirectoryInfo(val inputDirectories: List[String], val outputDirectory: String) {
  require(inputDirectories.nonEmpty)
}

class SlaveStartupInfo(val masterAddress: InetSocketAddress, val ioDirectoryInfo: IODirectoryInfo)

abstract class Identity
case object Master extends Identity {
  override def toString: String = "Master"
}
case class Slave(index: Int) extends Identity {
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

  def findSlaveIndexForKey(key: Key) = {
    def binarySearch(l: Int, r: Int): Int = {
      if (l < r) {
        val m = (l + r) >> 1
        if (slaveRanges(m).startKey <= key) binarySearch(m+1, r)
        else binarySearch(l, m)
      } else l-1
    }
    binarySearch(1, slaveRanges.length)
  }
}

trait State[T] {
  def run(): Future[T]
}

trait ConnectedWorkers {
  val partitionTable: PartitionTable
  val channelTable: ChannelTable
}