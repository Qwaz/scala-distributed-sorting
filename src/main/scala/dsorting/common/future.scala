package dsorting.common

import scala.concurrent._

package object future {
  implicit class FutureCompanionOps(val x: Future.type) extends AnyVal {
    def run()(f: CancellationToken => Future[Unit]): Subscription = {
      val subscription = CancellationTokenSource()
      f(subscription.cancellationToken)
      subscription
    }
  }

  implicit class FutureOps[T](val x: Future[T]) extends AnyVal {
    def ->[S](f: T => S)(implicit ec: ExecutionContext) = x.map(f)
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
}