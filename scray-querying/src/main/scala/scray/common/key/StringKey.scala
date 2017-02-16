package scray.common.key

class StringKey(value: String, version: Float = 1.0f) extends ScrayKey(value, version) with Serializable {

  val key = value

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

object StringKey extends KeyGenerator[String] {
  def apply(value: String, version: Float = 1.0f): StringKey = {
    new StringKey(value, version)
  }
}