package scray.common.key

import scala.util.Sorting

/**
 * Create a key from a Array[String]. 
 * The ordering of elements in the array is ignored.
 */
class OrderedStringKey(value: Array[String]) extends ScrayKey(value, 1.0f) with Serializable {
  
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
        that.getKeyAsString == this.getKeyAsString && that.version == this.version
      }
      case _ => false
    }
  }

  override def hashCode: Int = {
    key.hashCode()
  }

}