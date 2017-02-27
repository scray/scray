package scray.common.key.api

trait KeyGenerator[T] {
  
  def apply(valut: T, version: Float = 1.0f): ScrayKey[T]
  
}