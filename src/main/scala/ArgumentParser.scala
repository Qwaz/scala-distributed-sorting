package dsorting.transition

import dsorting.primitive._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait ArgumentParser[T] extends Transition[Array[String], T] {
  def apply(args: Array[String]): Future[T]
}

class MasterArgumentParser extends ArgumentParser[Integer] {
  def apply(args: Array[String]): Future[Integer] = Future {
    if (args.length != 1) throw new IllegalArgumentException("Argument size must be 1")
    args(0).toInt
  }
}

class SlaveArgumentParser extends ArgumentParser[IODirectoryInfo] {
  def apply(args: Array[String]): Future[IODirectoryInfo] = Future {
    val indexI = args.indexOf("-I")
    val indexO = args.indexOf("-O", indexI)
    new IODirectoryInfo(args.slice(indexI+1, indexO).toList, args(indexO+1))
  }
}
