package dsorting.entrypoint

import scala.concurrent.ExecutionContext.Implicits.global

import dsorting.common.future._

// sbt "run arg1 arg2 arg3"

object MasterApp extends App {
  import dsorting.transition.master._

  parseArgument(args) -> prepareSampling -> {
    samplingState =>
      samplingState.run() -> preparePartitioning(samplingState) -> {
        _ => ()
      }
  }
}

object SlaveApp extends App {
  import dsorting.transition.slave._

  parseArgument(args) -> prepareSampling -> {
    _ => ()
  }
}
