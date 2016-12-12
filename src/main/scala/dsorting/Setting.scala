package dsorting

import java.io.IOException
import java.net.{InetAddress, InetSocketAddress, Socket}

object Setting {
  /*
  http://stackoverflow.com/a/13826145
  port availability check
   */
  private def portAvailable(port: Int) = {
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

  private def nextAvailablePort(port: Int): Int = {
    if (portAvailable(port)) port
    else nextAvailablePort(port+1)
  }

  val MessageLoggingEnabled = false

  def MasterPort = 25252
  def SlavePort = nextAvailablePort(3939)

  val BufferSize = 1024 * 1024 * 1 // 1 MiB

  val KeySize = 10
  val ValueSize = 90
  val EntrySize = KeySize + ValueSize

  val ShufflingChunkSize = EntrySize * 3000
  assert(ShufflingChunkSize % EntrySize == 0)

  val InputFilePrefix = "input"

  val NumSamples = 10
}