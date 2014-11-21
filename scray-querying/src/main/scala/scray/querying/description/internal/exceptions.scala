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
package scray.querying.description.internal

import scray.querying.description.Column
import scray.querying.Query
import java.util.UUID
import scray.querying.queries.DomainQuery
import scray.common.exceptions.ScrayException
import scray.querying.ExceptionIDs

class QueryDomainParserException(reason: QueryDomainParserExceptionReasons.Reason, column: Column, query: Query) 
    extends ScrayException(ExceptionIDs.queryDomainParserExceptionID, Some(query.getQueryID), 
        s"Could not parse query domains for column:${column.columnName}, reason is:$reason")

object QueryDomainParserExceptionReasons extends Enumeration with Serializable {
  type Reason = Value
  val DISJOINT_EQUALITY_CONFLICT, // reason: col1 = a and col1 = b   
  DOMAIN_EQUALITY_CONFLICT, // example reason: col1 < a and col1 = b, but b >= a 
  DOMAIN_DISJOINT_CONFLICT, // example reason: col1 < a and col1 > b, but b >= a 
  UNKNOWN_DOMAIN_CONFLICT = Value // unknown reason
}

class QueryDomainRangeException(column: Column, query: DomainQuery) extends ScrayException(ExceptionIDs.queryDomainRangeException, Some(query.getQueryID), 
        s"Could not execute tome-based index on column:${column.columnName}, reason is that the domain with a range has no bounds at all.") with Serializable

class QueryWithoutColumnsException(query: Query) 
    extends ScrayException(ExceptionIDs.queryWithoutColumnsExceptionID, Some(query.getQueryID), "query contains no columns to query") with Serializable

class QueryspaceViolationException(query: Query)
    extends ScrayException(ExceptionIDs.queryspaceViolationExceptionID, Some(query.getQueryID), s"""query trys to access queryspace or table which has not
    been registered; queryspace=${query.getQueryspace}, table=${query.getTableIdentifier} """) with Serializable

class QueryspaceColumnViolationException(query: Query, column: Column) 
    extends ScrayException(ExceptionIDs.queryspaceColumnViolationExceptionID, Some(query.getQueryID), s"""query trys to access column ${column.columnName} from 
    queryspace ${query.getQueryspace} which has not been registered""") with Serializable

class KeyBasedQueryException(query: DomainQuery) 
    extends ScrayException(ExceptionIDs.keyBasedQueryExceptionID, Some(query.getQueryID), s"""query trys to access queryspace
    ${query.getQueryspace} which has not been registered""") with Serializable

class NonAtomicClauseException(query: Query)
    extends ScrayException(ExceptionIDs.nonAtomicClauseExceptionID, Some(query.getQueryID), "To qualify predicates those must be either be atomic clauses or 'And's")
    with Serializable

class NoPlanException(query: Query)
    extends ScrayException(ExceptionIDs.noPlanExceptionID, Some(query.getQueryID), "Could not construct a plan from the query") with Serializable

class ExecutorShutdownException(query: Query)
    extends ScrayException(ExceptionIDs.plannerShutdownExceptionID, Some(query.getQueryID), "Cannot accept queries any more. Engine shut down.") with Serializable

class IndexTypeException(query: Query)
    extends ScrayException(ExceptionIDs.indexTypeExceptionID, Some(query.getQueryID), s"""Index type is not defined or not
    available in queryspace ${query.getQueryspace}""") with Serializable

class WrongQueryTypeForCacheException(query: DomainQuery, sourceDiscriminant: String)
    extends ScrayException(ExceptionIDs.wrongQueryTypeForCacheID, Some(query.getQueryID), s"""Different type query was expected for 
    this cache for source ${sourceDiscriminant} on query ${query.getQueryspace}""") with Serializable

    