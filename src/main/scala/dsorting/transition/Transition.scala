package dsorting.transition

import scala.concurrent.Future

trait Transition[-A, +B] {
  def apply(state: A): Future[B]
}