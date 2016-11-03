package dsorting.common

import java.io.IOException
import java.net.{InetAddress, InetSocketAddress, Socket}

object Setting {
  /*
  http://stackoverflow.com/a/13826145
  port availability check
   */
  private def portAvailable(port: Integer) = {
    val s = new Socket()
    try {
      s.connect(new InetSocketAddress(InetAddress.getLocalHost, port))
      false
    } catch {
      case e: IOException => true
    } finally {
      if (s != null) {
        try {
          s.close()
        } catch {
          case e: IOException => ()
        }
      }
    }
  }

  private def nextAvailablePort(port: Integer): Integer = {
    if (portAvailable(port)) port
    else nextAvailablePort(port+1)
  }

  val MessageLoggingEnabled = false

  def MasterPort = nextAvailablePort(25252)
  def SlavePort = nextAvailablePort(3939)

  val BufferSize = 4096

  val KeySize = 10
}