package scray.common.key


abstract class ScrayKey[T](value: T) {

  def getKeyAsString: String

  override def equals(other: Any): Boolean
  override def hashCode: Int
}
