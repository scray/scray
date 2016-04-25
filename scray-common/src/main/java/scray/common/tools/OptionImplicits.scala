package scray.common.tools

object OptionImplicits {

  implicit class BooleanOption(a: Boolean) {
    def toOption = if(a) Some(true) else None
  }
  
  implicit class OptionImplicits[T](a: Option[T]) {
    def &&[U](b: => Option[U]): Option[(T, U)] = {
      if(a.isDefined) {
        b.map(bval => (a.get, bval))   
      } else {
        None
      }
    }
    def ||[U](b: => Option[U]): Option[Either[T, U]] = {
      a.map(Left(_)).orElse(b.map(Right(_)))
    }
  }
}