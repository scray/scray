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
package scray.cassandra.extractors

import scray.querying.Registry
import com.typesafe.scalalogging.LazyLogging
import scray.querying.description.{Column, ColumnOrdering, TableIdentifier}
import scray.querying.description.internal.{Domain, RangeValueDomain, SingleValueDomain}
import scray.querying.description.{ColumnOrdering, TableIdentifier}
import scray.querying.queries.DomainQuery

/**
 * performs mapping of DomainQueries to valid JSON Lucene queries,
 * which in turn can be used by DomainToJSONLuceneQueryMapping
 */
object DomainToJSONLuceneQueryMapper extends LazyLogging {
  
  private def convertSingleValueDomain(vdomain: SingleValueDomain[_]): String = {
    if(vdomain.isNull) {
      ""
    } else if(vdomain.isWildcard) {
       val search = vdomain.value.toString()
       // TODO/optimize: replace **, *?, etc by *
       if((search.endsWith("*") || search.endsWith("?")) && search.length() > 1) {
         val search2 : String = search.substring(0, search.length() -1)
         if(!search2.contains("*") && !search2.contains("?")) {
           return s""" { type  : "prefix", field : "${vdomain.column.columnName}", value : "${search2}" } """
         }
       }
       s""" { type  : "wildcard", field : "${vdomain.column.columnName}", value : "${vdomain.value}" } """
    } else {
      s""" { type : "match", field : "${vdomain.column.columnName}", value : "${vdomain.value}" } """
    }
  }

  private def convertRangeValueDomain(vdomain: RangeValueDomain[_]): String = {
    val result = new StringBuilder
    result ++= ""
    if(vdomain.lowerBound.isDefined || vdomain.upperBound.isDefined) {
      result ++= s""" { type : "range", field : "${vdomain.column.columnName}", """
      vdomain.lowerBound.map { bound =>
        result ++= s""" lower: "${bound.value}" , include_lower: "${bound.inclusive}" """
        vdomain.upperBound.map ( _ => result ++= "," )
      }
      vdomain.upperBound.map { bound =>
        result ++= s""" upper: "${bound.value}" , include_upper: "${bound.inclusive}" """
      }
      result ++= s""" } """
    }
    result.toString
  }

  private def domainToQueryString(domain: Domain[_]): String = domain match {
    case single: SingleValueDomain[_] => convertSingleValueDomain(single)
    case range: RangeValueDomain[_] => convertRangeValueDomain(range)
  }

  private def sortIndex(optOrdering: Option[ColumnOrdering[_]], domains: List[Domain[_]]): String = {
    optOrdering.map { ordering =>
          s""" sort : { fields : [ { field : "${ordering.column.columnName}" , reverse : ${ordering.descending} } ] } """
    }.getOrElse("")
  }

  def getLuceneColumnsQueryMapping(query: DomainQuery, domains: List[Domain[_]], ti: TableIdentifier): Option[String] = {
    logger.debug("Create lucene code for " + query)
    val START_LUCENE_EXPRESSION = " lucene='{ ";
    val result = new StringBuilder
    // check for those domains only containing garbage
    val validDomains = domains.filter { dom =>
        dom.column.table.dbId == ti.dbId &&
        dom.column.table.tableId == ti.tableId &&
        dom.column.table.dbSystem == ti.dbSystem
      }.map(domainToQueryString(_)).filter(_ != "")
    if(validDomains.size > 0 | query.getOrdering.isDefined) {
      result ++= START_LUCENE_EXPRESSION
      
      if(validDomains.size > 0) {
        if(validDomains.size > 1) {
          result ++= """filter : { type: "boolean", must :["""
          result ++= validDomains.mkString(" , ")
          result ++= """]}"""
        } else {
          result ++= "filter :" + validDomains.head
        }
      }
      
      if(query.getOrdering.isDefined) {
        // Sperate JSON objects by comma
        if(result.endsWith(START_LUCENE_EXPRESSION)) {
          result ++= sortIndex(query.getOrdering, domains)
        } else {
          result ++= ", " + sortIndex(query.getOrdering, domains)
        }
      }
      result ++= " }' "
      Some(result.toString)
    } else {
      None
    }
  }
}
