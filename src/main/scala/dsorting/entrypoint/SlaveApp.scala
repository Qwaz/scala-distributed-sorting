package dsorting.entrypoint

import dsorting.future._
import dsorting.transition.slave._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

object SlaveApp extends App {
  val p = Promise[Unit]()

  val samplingState =
    ArgumentParser.parseArgument(args) -> SamplingInitializer.prepareSampling
  val partitioningState = samplingState flatMap {
    state => state.run() -> PartitioningInitializer.preparePartitioning(state)
  }
  val shufflingState = partitioningState flatMap {
    state => state.run() -> ShufflingInitializer.prepareShuffling(state)
  }
  val result = shufflingState flatMap {
    state => state.run() -> { _ => p.success(()) }
  }
  result onFailure { case e => p.tryFailure(e) }

  Await.result(p.future, Duration.Inf)
}
