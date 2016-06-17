package scray.querying.sync.types

trait MonoidF[T] {

  def zero: T
  def plus(left: T, right: T): T
}