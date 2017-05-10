package scray.common.key

import scala.util.Sorting
import scray.common.key.api.KeyGenerator
import scray.common.key.api.ScrayKey
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * Create a key of Array[String].
 * If array is empty return '_' 
 * The ordering of elements in the array is ignored.
 */
class OrderedStringKey(value: Array[String], version: Float = 1.0f) extends ScrayKey(value, version) with Serializable with LazyLogging {
  
  Sorting.quickSort(value)
  val key = generateKey(value).getOrElse("_")

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

  /*
   * Concatenate all elements with '_' in betweeen.
   * If array is empty return None
   */
  private def generateKey(value: Array[String]) = {
    val key = value.foldLeft("")((acc, element) => {
      if (acc.size < 1) {
        element
      } else {
        acc + "_" + element
      }
    })
    
    if(key.size < 1) {
      None
    } else {
      Some(key)
    }
  }

}

object OrderedStringKeyGenerator extends KeyGenerator[Array[String]] {

    def apply(value: Array[String], version: Float = 1.0f): OrderedStringKey = {
      new OrderedStringKey(value, version)
    }

}
