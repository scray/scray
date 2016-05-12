package scray.example.adapter

class Share(
    name: String, 
    l10: Option[Float], 
    a00: Option[Float], 
    b00: Option[Float], 
    g00: Option[Float], 
    h00: Option[Float]) {
  override def toString(): String = {
    s"Share {${name} \t ${l10} \t ${a00} \t ${b00} \t ${g00} \t ${h00}}"
  }
}
