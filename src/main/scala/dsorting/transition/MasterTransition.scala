package dsorting.transition

import dsorting.states.master._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

package object master {
  def parseArgument(args: Array[String]): Future[Integer] = Future {
    if (args.length != 1) throw new IllegalArgumentException("Argument size must be 1")
    args(0).toInt
  }

  def initializeState(numSlaves: Integer): Future[SamplingState] = Future {
    new SamplingState(numSlaves)
  }
}