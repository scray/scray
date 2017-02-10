package scray.common.key

/**
 * Some data produced by an aggregation job are identified by key.
 * To query this data the scray service requires this key. 
 * This interface can help to use the same keygeneration class.
 */
abstract class ScrayKey[T](value: T, val version: Float) {
  
  /**
   * Provide a string representation of the key.
   * Is used by scray service to query the data.
   */
  def getKeyAsString: String
  

  /**
   * Check if two objects are equals.
   * Is used by Spark
   */
  override def equals(other: Any): Boolean
  
  
  /**
   * Provides a hash code for this class. 
   * Is used by Spark
   */
  override def hashCode: Int
}
