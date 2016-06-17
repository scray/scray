package scray.querying.sync.types

import scalaz.Monoid

abstract class ScalazMonoid[T](val zero: T) extends MonoidF[T] with scalaz.Monoid[T] {
  
  @Override
  def plus(left: T, right: T): T = {
    this.append(left, this.zero)
  }

}