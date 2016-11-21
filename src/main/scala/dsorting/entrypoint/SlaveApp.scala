package dsorting.entrypoint

import dsorting.future._
import dsorting.transition.slave._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

object SlaveApp extends App {
  val p = Promise[Unit]()
  ArgumentParser.parseArgument(args) ->
    SamplingInitializer.prepareSampling flatMap {
    samplingState => samplingState.run() ->
      ShufflingInitializer.prepareShuffling(samplingState) flatMap {
      shufflingState => shufflingState.run() ->
        { _ => p.success(()) }
    }
  } onFailure { case e => p.tryFailure(e) }
  Await.result(p.future, Duration.Inf)
}
