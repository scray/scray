package scray.querying.sync

trait DbQueries {
  def createTableStatement[T <: AbstractRow](table: Table[T]): Option[String] 
}