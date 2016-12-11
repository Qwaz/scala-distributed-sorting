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
  val shufflingState = samplingState flatMap {
    state => state.run() -> ShufflingInitializer.prepareShuffling(state)
  }
  val result = shufflingState flatMap {
    state => state.run() -> { _ => p.success(()) }
  }
  result onFailure { case e => p.tryFailure(e) }

  Await.result(p.future, Duration.Inf)
}
