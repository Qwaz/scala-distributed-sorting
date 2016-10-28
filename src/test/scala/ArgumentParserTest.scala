package dsorting

import dsorting.transition._
import org.junit.runner.RunWith
import org.scalatest.AsyncFlatSpec
import org.scalatest.junit.JUnitRunner

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

@RunWith(classOf[JUnitRunner])
class ArgumentParserSuite extends AsyncFlatSpec {
  behavior of "MasterArgumentParser"

  it should "parse one integer argument" in {
    val parser = new MasterArgumentParser
    val argumentArray = Array("42")
    parser(argumentArray) map { res => assert(res == 42) }
  }

  behavior of "SlaveArgumentParser"

  def slaveArgumentArray = Array("-I", "a", "b", "c", "-O", "d")

  it should "parse input directory correctly" in {
    val parser = new SlaveArgumentParser
    parser(slaveArgumentArray) map { ioDirectoryInfo => assert(
      ioDirectoryInfo.inputFiles == "a" :: "b" :: "c" :: Nil
    ) }
  }

  it should "parse output directory correctly" in {
    val parser = new SlaveArgumentParser
    parser(slaveArgumentArray) map { ioDirectoryInfo => assert(
      ioDirectoryInfo.outputDirectory == "d"
    ) }
  }
}
