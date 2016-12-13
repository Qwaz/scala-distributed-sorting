package dsorting.entrypoint

import dsorting.future._
import dsorting.transition.master._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

object MasterApp extends App {
  val p = Promise[Unit]()

  val samplingState =
    ArgumentParser.parseArgument(args) -> SamplingInitializer.prepareSampling
  val partitioningState = samplingState flatMap {
    state => state.run() -> PartitioningInitializer.preparePartitioning(state)
  }
  val shufflingState = partitioningState flatMap {
    state => state.run() -> ShufflingInitializer.prepareShuffling(state)
  }
  val sortingState = shufflingState flatMap {
    state => state.run() -> SortingInitializer.prepareSorting(state)
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
