// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package scray.jdbc.extractors


import com.typesafe.scalalogging.slf4j.LazyLogging

import DomainToSQLQueryMapping.AND_LITERAL
import DomainToSQLQueryMapping.DESC_LITERAL
import DomainToSQLQueryMapping.EMPTY_LITERAL
import DomainToSQLQueryMapping.ORDER_LITERAL
import DomainToSQLQueryMapping.GROUP_LITERAL
import scray.jdbc.JDBCQueryableSource
import scray.querying.description.Column
import scray.querying.description.TableIdentifier
import scray.querying.description.internal.Domain
import scray.querying.description.internal.RangeValueDomain
import scray.querying.description.internal.SingleValueDomain
import scray.querying.queries.DomainQuery

import java.util.UUID
import java.sql.PreparedStatement
import scray.querying.description.internal.Bound


/**
 * performs mapping of DomainQueries to valid SQL (dependent on dialect) queries,
 * containing all the possible predicates for a single table as defined
 * in the domains
 */
class DomainToSQLQueryMapping[Q <: DomainQuery, S <: JDBCQueryableSource[Q]] extends LazyLogging {

  private def inflateColumns(column: Column): String = column.columnName
  
  /**
   * returns a formatted group by clause
   */
  private def getGroupBy(query: DomainQuery): String = query.grouping.map { grouping => 
    s"${GROUP_LITERAL} ${inflateColumns(grouping.column)}"
  }.getOrElse("")
 
  /**
   * returns a formatted order by clause
   */
  private def getOrderBy(query: DomainQuery): String = query.ordering.map { ordering => 
    if(ordering.descending) {
      s"${ORDER_LITERAL} ${DESC_LITERAL} "
    } else {
      s"${ORDER_LITERAL} "
    } + inflateColumns(ordering.column)
  }.getOrElse("")
 
  
  /**
   * if queries need to support more value types, you can add them in here
   */
  private def switchAndSetValueType[T](value: T, stmt: PreparedStatement, pos: Int): Unit = {
    value match {
      case i: Int => stmt.setInt(pos, i)
      case i: Integer => stmt.setInt(pos, i)
      case l: Long => stmt.setLong(pos, l)
      case sh: Short => stmt.setShort(pos, sh)
      case f: Float => stmt.setFloat(pos, f)
      case d: Double => stmt.setDouble(pos, d)
      case u: UUID => stmt.setString(pos, u.toString())
      case s: String => stmt.setString(pos, s)
    }
  }
    
  /**
   * maps a SingleValueDomain to a String with a question mark for further
   * processing with PreparedStaments
   */
  private def mapSingleValueDomainQuestion[T](domain: SingleValueDomain[T]): (String, Int) = domain.isNull match {
    case true => (s""" ${domain.column.columnName} IS NULL """, 0)
    case false => domain.isWildcard match {
      case true => (s""" ${domain.column.columnName} LIKE ? """, 1)
      case false => (s""" ${domain.column.columnName} = ? """, 1)
    } 
  }
  
  /**
   * maps a SingleValueDomain to a String with a question mark for further
   * processing with PreparedStaments
   */
  private def mapSingleValueDomainValue[T](domain: SingleValueDomain[T], stmt: PreparedStatement, pos: Int): Int = {
    if(!domain.isNull) {
      switchAndSetValueType(domain.value, stmt, pos)
      pos + 1
    } else pos
  }
  
  /**
   * maps 
   */
  private def mapRangeValueDomainValues[T](domain: RangeValueDomain[T], stmt: PreparedStatement, pos: Int): Int = {
    val lowercount = domain.lowerBound.map { bound => 
      switchAndSetValueType(bound.value, stmt, pos)
      pos + 1
    }.getOrElse(pos)
    val upperCount = domain.upperBound.map { bound =>
      switchAndSetValueType(bound.value, stmt, lowercount)
      1 + lowercount
    }.getOrElse(lowercount)
    domain.unequalValues.foldLeft(upperCount) { (acc, value) =>
      switchAndSetValueType(value, stmt, acc)
      acc + 1
    }
  }
  
  /**
   * maps a RangeValueDomain to a String with question marks for further
   * processing with PreparedStaments
   */
  private def mapRangeValueDomainQuestion[T](range: RangeValueDomain[T], dialect: ScraySQLDialect): (String, Int) = {
    def includeEqual(bound: Bound[_]): String = if(bound.inclusive) "=" else ""
    val domainString = {
      val lowerBound = range.lowerBound.map { bound =>
        s""" ${range.column.columnName} >${includeEqual(bound)} ? """
      }.getOrElse("")
      val upperBound = range.upperBound.map { bound =>
        s""" ${range.column.columnName} <${includeEqual(bound)} ? """
      }.getOrElse("")
      if(lowerBound.length() > 0 && upperBound.length() > 0) {
        s"""${lowerBound}${DomainToSQLQueryMapping.AND_LITERAL}${upperBound}"""
      } else {
        s"""${lowerBound}${upperBound}"""
      }
    }
    val domainCount = range.lowerBound.map(_ => 1).getOrElse(0) + range.upperBound.map(_ => 1).getOrElse(0)
    val rangeString = range.unequalValues.map(obj => s""" ${range.column.columnName} ${dialect.getUnequal} ? """).mkString(DomainToSQLQueryMapping.AND_LITERAL)
    val whereString = if(domainString.length > 0 && rangeString.length() > 0) {
      s""" ${domainString}${DomainToSQLQueryMapping.AND_LITERAL}${rangeString} """
    } else {
      if(rangeString.length > 0) {
        rangeString 
      } else {
        domainString
      }
    }
    (whereString, domainCount + range.unequalValues.size) 
  }
  
  /**
   * maps the where clause from Domains to a String which can be fed into a PreparedStatement
   */
  private def mapWhereClausesAndQuestions(domains: List[Domain[_]], dialect: ScraySQLDialect): (String, Int) = {
    val domTuple = domains.map { domain =>
      domain match {
        case single: SingleValueDomain[_] => mapSingleValueDomainQuestion(single)
        case range: RangeValueDomain[_] => mapRangeValueDomainQuestion(range, dialect)
        case _ => ("", 0)
      }
    }.unzip(a => (a._1, a._2))
    (domTuple._1.mkString(DomainToSQLQueryMapping.AND_LITERAL), domTuple._2.reduce(_ + _))
  }
  
  /**
   * sets the prepared question marks by executing sets on the given preparedStatement
   */
  def mapWhereClauseValues(stmt: PreparedStatement, domains: List[Domain[_]]): Unit = {
    domains.foldLeft(1) { (acc, domain) =>
      domain match {
        case single: SingleValueDomain[_] => mapSingleValueDomainValue(single, stmt, acc)
        case range: RangeValueDomain[_] => mapRangeValueDomainValues(range, stmt, acc)
        case _ => acc
      }
    }
  }
  
  /**
   * returns a function mapping from Domains to SQL-Strings used in Where clauses
   */
  def getQueryMapping(store: S, storeTableNickName: Option[String], dialect: ScraySQLDialect): DomainQuery => (String, Int) = {
    (query) =>
      {
        val whereClauseSQLString = mapWhereClausesAndQuestions(query.domains, dialect)
        val limit = dialect.getEnforcedLimit(query.range, query.domains)
        val select = dialect.getFormattedSelectString(store.ti, whereClauseSQLString._1, limit, getGroupBy(query), getOrderBy(query))
        logger.debug(s"SQL query string: $select")
        (select, whereClauseSQLString._2)
      }
  }
  
}

object DomainToSQLQueryMapping {
  val EMPTY_LITERAL: String = ""
  val AND_LITERAL: String = " AND "
  val ORDER_LITERAL: String = " ORDER BY "
  val GROUP_LITERAL: String = " GROUP BY "
  val DESC_LITERAL: String = " DESC "
}
