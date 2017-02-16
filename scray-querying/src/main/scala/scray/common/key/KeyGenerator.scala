package scray.common.key

trait KeyGenerator[T] {
  
  def apply(valut: T, version: Float = 1.0f): ScrayKey[T]
  
}