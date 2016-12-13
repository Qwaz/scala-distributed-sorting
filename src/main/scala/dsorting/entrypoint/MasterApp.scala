package dsorting.entrypoint

import dsorting.future._
import dsorting.states.master._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

object MasterApp extends App {
  val p = Promise[Unit]()

  val samplingState =
    ArgumentParser.parseArgument(args) -> SamplingStateFactory.apply
  val partitioningState = samplingState flatMap {
    state => state.run() -> PartitioningStateFactory(state)
  }
  val shufflingState = partitioningState flatMap {
    state => state.run() -> ShufflingStateFactory(state)
  }
  val sortingState = shufflingState flatMap {
    state => state.run() -> SortingStateFactory(state)
  }
  val result = sortingState flatMap {
    state => state.run() -> {
      childList =>
        println(childList.mkString(", "))
        p.success(())
    }
  }
  result onFailure { case e => p.tryFailure(e) }

  Await.result(p.future, Duration.Inf)
}
