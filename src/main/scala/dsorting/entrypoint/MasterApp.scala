package dsorting.entrypoint

import dsorting.common.future._
import dsorting.transition.master._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

object MasterApp extends App {
  val p = Promise[Unit]()
  parseArgument(args) -> prepareSampling -> {
    samplingState =>
      samplingState.run() -> preparePartitioning(samplingState) -> {
        partitioningState =>
          partitioningState.run() -> {
            _ => p.success(())
          } onFailure { case e => p.tryFailure(e) }
      } onFailure { case e => p.tryFailure(e) }
  } onFailure { case e => p.tryFailure(e) }
  Await.result(p.future, Duration.Inf)
}
