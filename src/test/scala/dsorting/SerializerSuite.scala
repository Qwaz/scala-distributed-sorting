package dsorting

import java.net.InetSocketAddress

import dsorting.primitive._
import dsorting.serializer._
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SerializerSuite extends FlatSpec {
  {
    behavior of "InetSocketAddressSerializer"

    val socketAddress = new InetSocketAddress("192.168.111.222", 324)
    val clone = InetSocketAddressSerializer.fromByteArray(InetSocketAddressSerializer.toByteArray(socketAddress))

    it should "serialize host correctly" in {
      assert(socketAddress.getHostString == clone.getHostString)
    }

    it should "serialize port correctly" in {
      assert(socketAddress.getPort == clone.getPort)
    }
  }

  {
    behavior of "KeyListSerializer"

    val firstKey = new Key(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val secondKey = new Key(Array[Byte](54, 45, 2, 5, 7, 8, 9, 52, 1, 64))
    val thirdKey = new Key(Array[Byte](9, 8, 7, 6, 5, 4, 3, 2, 1, 0))
    val keyList = firstKey :: secondKey :: thirdKey :: Nil
    val clone = KeyListSerializer.fromByteArray(KeyListSerializer.toByteArray(keyList))

    it should "serialize key list correctly" in {
      def keyListToString(keyList: Seq[Key]): String = keyList match {
        case Nil => ""
        case key +: tail => key.toString + " / " + keyListToString(tail)
      }
      assert(keyListToString(keyList) == keyListToString(clone))
    }
  }

  {
    behavior of "PartitionTableSerializer"

    val tkey1 = new Key(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val tkey2 = new Key(Array[Byte](54, 45, 2, 5, 7, 8, 9, 52, 1, 64))

    val partitionTable = new PartitionTable(
      Slave(22),
      Vector[SlaveRange](new SlaveRange(new InetSocketAddress("111.111.111.111", 1111), tkey1), new SlaveRange(new InetSocketAddress("222.222.222.222", 2222), tkey2))
    )
    val clone = PartitionTableSerializer.fromByteArray(PartitionTableSerializer.toByteArray(partitionTable))

    it should "serialize identity correctly" in {
      assert(partitionTable.identity == clone.identity)
    }

    it should "serialize slave range correctly" in {
      assert(partitionTable.identity == clone.identity)
    }
  }
}
