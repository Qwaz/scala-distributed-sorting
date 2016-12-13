package dsorting

import org.junit.runner.RunWith
import org.scalatest.AsyncFlatSpec
import org.scalatest.junit.JUnitRunner

import scala.concurrent.ExecutionContext.Implicits.global

@RunWith(classOf[JUnitRunner])
class ParseArgumentSuite extends AsyncFlatSpec {
  {
    import dsorting.states.master.ArgumentParser

    behavior of "ArgumentParser (master)"

    it should "parse one integer argument" in {
      val argumentArray = Array("42")
      ArgumentParser.parseArgument(argumentArray) map { res => assert(res == 42) }
    }

    it should "reject multiple arguments" in {
      val argumentArray = Array("1", "2", "3")
      recoverToSucceededIf[IllegalArgumentException] {
        ArgumentParser.parseArgument(argumentArray)
      }
    }
  }

  {
    import dsorting.states.slave.ArgumentParser

    behavior of "ArgumentParser (slave)"

    def slaveArgumentArray = Array("50.100.150.200:2525", "-I", "a", "b", "c", "-O", "d")

    it should "parse master address correctly" in {
      ArgumentParser.parseArgument(slaveArgumentArray) map { slaveStartupInfo => assert(
        slaveStartupInfo.masterAddress.getHostString == "50.100.150.200"
      )
      }
    }

    it should "parse master port correctly" in {
      ArgumentParser.parseArgument(slaveArgumentArray) map { slaveStartupInfo => assert(
        slaveStartupInfo.masterAddress.getPort == 2525
      )
      }
    }

    it should "parse input directories correctly" in {
      ArgumentParser.parseArgument(slaveArgumentArray) map { slaveStartupInfo => assert(
        slaveStartupInfo.ioDirectoryInfo.inputDirectories == "a" :: "b" :: "c" :: Nil
      )
      }
    }

    it should "parse ourput directory correctly" in {
      ArgumentParser.parseArgument(slaveArgumentArray) map { slaveStartupInfo => assert(
        slaveStartupInfo.ioDirectoryInfo.outputDirectory == "d"
      )
      }
    }
  }
}
