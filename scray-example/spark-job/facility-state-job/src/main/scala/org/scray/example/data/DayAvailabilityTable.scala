package de.s_node.analyser.data

import scray.cassandra.sync.CassandraImplementation._
import scray.querying.sync.Columns
import scray.querying.sync.Table
import scray.querying.sync.RowWithValue
import scray.querying.sync.ColumnWithValue
import scray.querying.sync.RowWithValue

object DayAvailabilityTable  {
  
  val facility = new ColumnWithValue[String]("facility", "")
  val time = new ColumnWithValue[Long]("time", 0L)
  val availability = new ColumnWithValue[Int]("availability", 0)
  
  val columns = new Columns(
      facility ::
      time :: 
      availability :: 
      Nil, "((facility), time)", None)
  val table = new Table("\"www.db.opendata.s-node.de\"", "\"DayAvailabilityTable\"", columns, Some("{'class' : 'NetworkTopologyStrategy', 'DC1' : 0, 'DC2' : 1}"))
    
  val row = new RowWithValue(facility :: time :: availability :: Nil, "(facility)", None)
    
  def setAvailability(facility: String, time: Long, availability: Int) = {
        new RowWithValue(
        new ColumnWithValue[String]("facility"  , facility) :: 
        new ColumnWithValue[Long]("time"  , time) :: 
        new ColumnWithValue("availability", availability) :: 
        Nil, 
        "((facility), time)", None
    )}
}