package scray.example.adapter

class Share(
    val name: String, 
    val l10: Option[Float], 
    val a00: Option[Float], 
    val b00: Option[Float], 
    val g00: Option[Float], 
    val h00: Option[Float]) {
  
  def this() {
    this("Stock", Some(1f), Some(1f), Some(1f), Some(1f), Some(1f))
  }
  
  override def toString(): String = {
    s"Share {${name} \t ${l10} \t ${a00} \t ${b00} \t ${g00} \t ${h00}}"
  }
}
