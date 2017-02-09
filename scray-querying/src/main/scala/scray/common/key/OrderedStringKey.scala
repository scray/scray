package scray.common.key

import scala.util.Sorting

class OrderedStringKey(value: Array[String]) extends ScrayKey(value) {
  
  Sorting.quickSort(value)
  val key = value.foldLeft("")((acc, element) => { 
      if(acc.size < 1) {
        element
      } else {
        acc + "_" + element
      }
    }
  )
  
  def getKeyAsString: String = {
    key
  }
  
  override def equals(other: Any): Boolean = {
    // FIXME
    true
  }
  
  override def hashCode: Int = {
    key.hashCode()
  }
  
}