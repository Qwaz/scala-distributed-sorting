package dsorting

import dsorting.transition._
import org.junit.runner.RunWith
import org.scalatest.AsyncFlatSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitRunner

import scala.concurrent.ExecutionContext.Implicits.global

@RunWith(classOf[JUnitRunner])
class ArgumentParserSuite extends AsyncFlatSpec {
  behavior of "MasterArgumentParser"

  it should "parse one integer argument" in {
    val parser = new MasterArgumentParser
    val argumentArray = Array("42")
    parser(argumentArray) map { res => assert(res == 42) }
  }

  it should "reject multiple arguments" in {
    val parser = new MasterArgumentParser
    val argumentArray = Array("1", "2", "3")
    recoverToSucceededIf[IllegalArgumentException] {
      parser(argumentArray)
    }
  }

  behavior of "SlaveArgumentParser"

  def slaveArgumentArray = Array("50.100.150.200:2525", "-I", "a", "b", "c", "-O", "d")

  it should "parse master address correctly" in {
    val parser = new SlaveArgumentParser
    parser(slaveArgumentArray) map { case (masterAddress, ioDirectoryInfo) => assert(
      masterAddress.getHostString == "50.100.150.200"
    ) }
  }

  it should "parse master port correctly" in {
    val parser = new SlaveArgumentParser
    parser(slaveArgumentArray) map { case (masterAddress, ioDirectoryInfo) => assert(
      masterAddress.getPort == 2525
    ) }
  }

  it should "parse input directory correctly" in {
    val parser = new SlaveArgumentParser
    parser(slaveArgumentArray) map { case (masterAddress, ioDirectoryInfo) => assert(
      ioDirectoryInfo.inputFiles == "a" :: "b" :: "c" :: Nil
    ) }
  }

  it should "parse output directory correctly" in {
    val parser = new SlaveArgumentParser
    parser(slaveArgumentArray) map { case (masterAddress, ioDirectoryInfo) => assert(
      ioDirectoryInfo.outputDirectory == "d"
    ) }
  }
}
