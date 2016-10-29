package dsorting.entrypoint

import scala.concurrent.ExecutionContext.Implicits.global

// sbt "run arg1 arg2 arg3"

object MasterApp extends App {
  import dsorting.transition.master._

  parseArgument(args) map initializeState
}

object SlaveApp extends App {
  import dsorting.transition.slave._

  parseArgument(args) map initializeState
}
