package dsorting

import org.junit.runner.RunWith
import org.scalatest.AsyncFlatSpec
import org.scalatest.junit.JUnitRunner

import scala.concurrent.ExecutionContext.Implicits.global

@RunWith(classOf[JUnitRunner])
class ParseArgumentSuite extends AsyncFlatSpec {
  {
    import dsorting.transition.master.parseArgument

    behavior of "parseArgument (master)"

    it should "parse one integer argument" in {
      val argumentArray = Array("42")
      parseArgument(argumentArray) map { res => assert(res == 42) }
    }

    it should "reject multiple arguments" in {
      val argumentArray = Array("1", "2", "3")
      recoverToSucceededIf[IllegalArgumentException] {
        parseArgument(argumentArray)
      }
    }
  }

  {
    import dsorting.transition.slave.parseArgument

    behavior of "parseArgument (slave)"

    def slaveArgumentArray = Array("50.100.150.200:2525", "-I", "a", "b", "c", "-O", "d")

    it should "parse master address correctly" in {
      parseArgument(slaveArgumentArray) map { slaveStartupInfo => assert(
        slaveStartupInfo.masterAddress.getHostString == "50.100.150.200"
      )
      }
    }

    it should "parse master port correctly" in {
      parseArgument(slaveArgumentArray) map { slaveStartupInfo => assert(
        slaveStartupInfo.masterAddress.getPort == 2525
      )
      }
    }

    it should "parse input directories correctly" in {
      parseArgument(slaveArgumentArray) map { slaveStartupInfo => assert(
        slaveStartupInfo.ioDirectoryInfo.inputFiles == "a" :: "b" :: "c" :: Nil
      )
      }
    }

    it should "parse ourpur directory correctly" in {
      parseArgument(slaveArgumentArray) map { slaveStartupInfo => assert(
        slaveStartupInfo.ioDirectoryInfo.outputDirectory == "d"
      )
      }
    }
  }
}
