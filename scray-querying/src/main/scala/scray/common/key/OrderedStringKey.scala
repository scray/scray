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

  override def equals(that: Any): Boolean = {
    that match {
      case that: OrderedStringKey => {
        that.getKeyAsString == this.getKeyAsString
      }
      case _ => false
    }
  }

  override def hashCode: Int = {
    key.hashCode()
  }

}