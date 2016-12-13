package dsorting.states.slave

import java.net.{InetAddress, InetSocketAddress, URI, URISyntaxException}

import dsorting.Setting

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object ArgumentParser {
  def parseArgument(args: Array[String]): Future[SlaveStartupInfo] = Future {
    def inetSocketAddressFromString(str: String): InetSocketAddress = {
      if (str == "local") {
        new InetSocketAddress(InetAddress.getLocalHost.getHostAddress, Setting.MasterPort)
      } else {
        /*
        http://stackoverflow.com/questions/2345063/java-common-way-to-validate-and-convert-hostport-to-inetsocketaddress
        Use custom scheme to parse host:port from a string
         */
        val uri = new URI("my://" + str)
        val host = uri.getHost
        val port = uri.getPort

        if (host == null || port == -1)
          throw new URISyntaxException(uri.toString, "URI must have host and port parts")
        else
          new InetSocketAddress(host, port)
      }
    }

    val indexI = args.indexOf("-I")
    val indexO = args.indexOf("-O", indexI)
    if (indexI != 1 || indexO != args.length - 2)
      throw new IllegalArgumentException("Usage: master_ip:port -I <input_directory 1> <input_directory 2> ... -O output_directory")
    else
      new SlaveStartupInfo(inetSocketAddressFromString(args(0)), new IODirectoryInfo(args.slice(indexI+1, indexO).toList, args(indexO+1)))
  }
}
