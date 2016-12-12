package dsorting

import java.io.{PrintWriter, StringWriter}

import com.typesafe.scalalogging.Logger

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

package object future {
  implicit class FutureCompanionOps(val x: Future.type) extends AnyVal {
    def run()(f: CancellationToken => Future[Unit]): Subscription = {
      val subscription = CancellationTokenSource()
      f(subscription.cancellationToken).logError("Background Exception")
      subscription
    }
  }

  implicit class FutureOps[T](val x: Future[T]) extends AnyVal {
    def ->[S](f: T => S)(implicit ec: ExecutionContext) = x.map(f)

    def logError(loggerName: String) = {
      x onFailure {
        case e =>
          val logger = Logger(loggerName)
          val sw = new StringWriter
          e.printStackTrace(new PrintWriter(sw))
          logger.error(sw.toString)
      }
    }
  }

  trait Subscription {
    def unsubscribe(): Unit
  }

  object Subscription {
    def apply(s1: Subscription, s2: Subscription) = new Subscription {
      def unsubscribe() {
        s1.unsubscribe()
        s2.unsubscribe()
      }
    }
  }

  trait CancellationToken {
    def isCancelled: Boolean
    def nonCancelled = !isCancelled
  }

  trait CancellationTokenSource extends Subscription {
    def cancellationToken: CancellationToken
  }

  object CancellationTokenSource {
    def apply() = new CancellationTokenSource {
      val p = Promise[Unit]()
      val cancellationToken = new CancellationToken {
        def isCancelled = p.future.value.isDefined
      }
      def unsubscribe() {
        p.trySuccess(())
      }
    }
  }

  class Futurama[T] {
    val pool = mutable.HashMap.empty[T, Future[Unit]]

    def getFuture(key: T): Future[Unit] = {
      if (!pool.contains(key))
        pool += (key -> Future())
      pool(key)
    }

    def executeImmediately[A](func: A => Unit, param: A): Future[Unit] = {
      Future { func(param) }
    }

    def executeAfter[A](key: T)(func: A => Unit, param: A): Future[Unit] = {
      pool.synchronized {
        val prevFuture = getFuture(key)
        val updatedFuture = prevFuture map {
          _ => func(param)
        }
        pool.update(key, updatedFuture)
        updatedFuture
      }
    }
  }
}