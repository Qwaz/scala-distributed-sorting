package dsorting.transition

import java.net.{InetSocketAddress, URI, URISyntaxException}
import javax.print.URIException

import dsorting.primitive._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait ArgumentParser[T] extends Transition[Array[String], T] {
  def apply(args: Array[String]): Future[T]
}

class MasterArgumentParser extends ArgumentParser[Integer] {
  def apply(args: Array[String]): Future[Integer] = Future {
    if (args.length != 1) throw new IllegalArgumentException("Argument size must be 1")
    args(0).toInt
  }
}

class SlaveArgumentParser extends ArgumentParser[SlaveStartupInfo] {
  private def inetSocketAddressFromString(str: String): InetSocketAddress = {
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

  def apply(args: Array[String]): Future[SlaveStartupInfo] = Future {
    val indexI = args.indexOf("-I")
    val indexO = args.indexOf("-O", indexI)
    if (indexI != 1 || indexO != args.length - 2)
      throw new IllegalArgumentException("Usage: master_ip:port -I <input_directory 1> <input_directory 2> ... -O output_directory")
    else
      (inetSocketAddressFromString(args(0)), new IODirectoryInfo(args.slice(indexI+1, indexO).toList, args(indexO+1)))
  }
}
