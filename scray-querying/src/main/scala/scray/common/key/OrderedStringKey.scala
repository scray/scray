package scray.common.key

import scala.util.Sorting

class OrderedStringKey(value: Array[String]) extends ScrayKey(value) {

  Sorting.quickSort(value)
  val key = value.foldLeft("")((acc, element) => {
    if (acc.size < 1) {
      element
    } else {
      acc + "_" + element
    }
  })

  def getKeyAsString: String = {
    key
  }

  override def equals(other: Any): Boolean = {
    if (other == null) {
      false
    }

    if (other == this) {
      true
    }

    if (!(other.isInstanceOf[OrderedStringKey])) {
      false
    }

    // other is a OrderedStringKey
    val otherKey = other.asInstanceOf[OrderedStringKey]

    return this.getKeyAsString == otherKey.getKeyAsString
  }

  override def hashCode: Int = {
    key.hashCode()
  }

}