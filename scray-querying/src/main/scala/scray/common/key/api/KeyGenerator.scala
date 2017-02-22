package scray.common.key.api

import scray.common.key.api.ScrayKey

trait KeyGenerator[T] {
  
  def apply(valut: T, version: Float = 1.0f): ScrayKey[T]
  
}