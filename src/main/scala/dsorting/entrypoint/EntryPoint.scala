package dsorting.entrypoint

// sbt "run arg1 arg2 arg3"

object MasterApp extends App {
  println("Hello from Master!")
  println("Args are:\n" + args.mkString("\n"))
}

object SlaveApp extends App {
  println("Hello from Slave...")
  println("Args are:\n" + args.mkString("\n"))
}
