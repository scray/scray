package scray.querying.sync.types

import com.twitter.algebird.Monoid

class AlgebirdMonoid[T](val zero: T) extends MonoidF[T] with com.twitter.algebird.Monoid[T] {
  
  @Override
  def plus(left: T, right: T): T = {
    left
  }
}