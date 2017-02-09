package scray.common.key


abstract class ScrayKey[T](value: T) {

  def getKeyAsString: String
  def equals(other: Object): Boolean
  def hashCode: Int
}
