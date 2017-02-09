package scray.common.key

import scala.util.Sorting

class OrderedStringKey(value: Array[String]) extends ScrayKey(value) {
  
  Sorting.quickSort(value)
  val key = value.foldLeft("")((acc, element) => acc + "_" + element)
  
  def getKeyAsString: String = {
    key
  }
  override def equals(other: Any): Boolean = {
    false
  }
  
  override def hashCode: Int = {
    1
  }
  
}