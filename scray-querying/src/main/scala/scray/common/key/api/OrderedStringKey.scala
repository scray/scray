package scray.common.key

import scala.util.Sorting
import scray.common.key.api.KeyGenerator
import scray.common.key.api.ScrayKey

/**
 * Create a key from a Array[String]. 
 * The ordering of elements in the array is ignored.
 */
class OrderedStringKey(value: Array[String], version: Float = 1.0f) extends ScrayKey(value, version) with Serializable {
  
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
    return key.hashCode()
  }

}

object OrderedStringKeyGenerator extends KeyGenerator[Array[String]] {

    def apply(value: Array[String], version: Float = 1.0f): OrderedStringKey = {
      new OrderedStringKey(value, version)
    }

}
