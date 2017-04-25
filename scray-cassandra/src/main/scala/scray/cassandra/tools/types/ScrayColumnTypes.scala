package scray.cassandra.tools.types

object ScrayColumnTypes {
  
  sealed trait ScrayColumnType
  case class String(value: java.lang.String) extends ScrayColumnType
  case class Long(value: java.lang.String) extends ScrayColumnType 
  case class Integer(value: java.lang.String) extends ScrayColumnType  
  case class Double(value: java.lang.String) extends ScrayColumnType  
  case class Float(value: java.lang.String) extends ScrayColumnType  
  case class Boolean(value: java.lang.String) extends ScrayColumnType  
}