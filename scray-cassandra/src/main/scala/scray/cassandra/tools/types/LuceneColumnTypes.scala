package scray.cassandra.tools.types

import scray.cassandra.tools.types.ScrayColumnTypes.ScrayColumnType
import scray.cassandra.tools.types.ScrayColumnTypes._
import com.typesafe.scalalogging.slf4j.LazyLogging

object LuceneColumnTypes extends LazyLogging {
  
  sealed trait LuceneColumnType
  case class String(name: java.lang.String, columnParams: Option[String] = None) extends LuceneColumnType {
  override def toString: java.lang.String = {
      "string"
    }
  }
  
  case class Integer(name: java.lang.String, columnParams: Option[String] = None) extends LuceneColumnType {
  override def toString: java.lang.String = {
      "integer"
    }
  }
  
  case class Long(name: java.lang.String, columnParams: Option[String] = None) extends LuceneColumnType {
  override def toString: java.lang.String = {
     "long"
    }
  }
  
  private def getAsJson(name: java.lang.String, luceneType: java.lang.String, columnParams: Option[String]): java.lang.String = {
    s"""${name}\t {type: "${luceneType}" ${columnParams.getOrElse("")}}"""
  }
  
  def getLuceneType(value: ScrayColumnType): Option[LuceneColumnType] = {

    value match {
      case column: ScrayColumnTypes.String => Some(LuceneColumnTypes.String(column.value, None))
      case column: ScrayColumnTypes.Long => Some(LuceneColumnTypes.Long(column.value, None))
      case column: ScrayColumnTypes.Integer => Some(LuceneColumnTypes.Integer(column.value, None))
      case unknownCasType => {logger.warn(s"No scray column type for ${unknownCasType} found."); None}
    }
  }
}