package dsorting.states.master

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object ArgumentParser {
  def parseArgument(args: Array[String]): Future[Int] = Future {
    if (args.length != 1) throw new IllegalArgumentException("Argument size must be 1")
    args(0).toInt
  }
}
