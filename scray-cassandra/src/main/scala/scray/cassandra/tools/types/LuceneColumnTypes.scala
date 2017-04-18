package scray.cassandra.tools.types

import scray.cassandra.tools.types.ScrayColumnType.ScrayColumnType
import scray.cassandra.tools.types.ScrayColumnType._

object LuceneColumnTypes {
  
  sealed trait LuceneColumnTypes
  case class String(name: java.lang.String, columnParams: Option[String]) extends LuceneColumnTypes {
  override def toString: java.lang.String = {
      getAsJson(name, "string", columnParams)
    }
  }
  
  case class Integer(name: java.lang.String, columnParams: Option[String]) extends LuceneColumnTypes {
  override def toString: java.lang.String = {
      getAsJson(name, "integer", columnParams)
    }
  }
  
  case class Long(name: java.lang.String, columnParams: Option[String]) extends LuceneColumnTypes {
  override def toString: java.lang.String = {
     getAsJson(name, "long", columnParams)
    }
  }
  
  private def getAsJson(name: java.lang.String, luceneType: java.lang.String, columnParams: Option[String]): java.lang.String = {
    s"""${name}\t {type: "${luceneType}" ${columnParams.getOrElse("")}}"""
  }
  
  def getLuceneType(value: ScrayColumnType): LuceneColumnTypes = {

    value match {
      case column: ScrayColumnType.String => LuceneColumnTypes.String(column.value, None)
      case column: ScrayColumnType.Long => LuceneColumnTypes.Long(column.value, None)
      case column: ScrayColumnType.Integer => LuceneColumnTypes.Integer(column.value, None)
    }
  }
}