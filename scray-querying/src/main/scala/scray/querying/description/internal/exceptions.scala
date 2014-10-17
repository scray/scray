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

object ExceptionIDs {
  // 000+ = query verification errors
  val queryWithoutColumnsExceptionID = "SIL-Scray-001"
  val queryspaceViolationExceptionID = "SIL-Scray-002"
  val queryspaceColumnViolationExceptionID = "SIL-Scray-003"
  // 010+ = planner errors
  val queryDomainParserExceptionID = "SIL-Scray-010"
  val nonAtomicClauseExceptionID = "SIL-Scray-011"
  val noPlanExceptionID = "SIL-Scray-012"
  val plannerShutdownExceptionID = "SIL-Scray-013"
  // 800+ = errors for specific queries
  val keyBasedQueryExceptionID = "SIL-Scray-800"
}

class ScrayException(id: String, query: UUID, msg: String) extends Exception(s"$id: $msg for query ${query}") with Serializable

class QueryDomainParserException(reason: QueryDomainParserExceptionReasons.Reason, column: Column, query: Query) 
    extends ScrayException(ExceptionIDs.queryDomainParserExceptionID, query.getQueryID, 
        s"Could not parse query domains for column:${column.columnName}, reason is:$reason") with Serializable

object QueryDomainParserExceptionReasons extends Enumeration with Serializable {
  type Reason = Value
  val DISJOINT_EQUALITY_CONFLICT, // reason: col1 = a and col1 = b   
  DOMAIN_EQUALITY_CONFLICT, // example reason: col1 < a and col1 = b, but b >= a 
  DOMAIN_DISJOINT_CONFLICT, // example reason: col1 < a and col1 > b, but b >= a 
  UNKNOWN_DOMAIN_CONFLICT = Value // unknown reason
}

class QueryWithoutColumnsException(query: Query) 
    extends ScrayException(ExceptionIDs.queryWithoutColumnsExceptionID, query.getQueryID, "query contains no columns to query") with Serializable

class QueryspaceViolationException(query: Query)
    extends ScrayException(ExceptionIDs.queryspaceViolationExceptionID, query.getQueryID, s"""query trys to access queryspace or table which has not
    been registered; queryspace=${query.getQueryspace}, table=${query.getTableIdentifier} """) with Serializable

class QueryspaceColumnViolationException(query: Query, column: Column) 
    extends ScrayException(ExceptionIDs.queryspaceColumnViolationExceptionID, query.getQueryID, s"""query trys to access column ${column.columnName} from 
    queryspace ${query.getQueryspace} which has not been registered""") with Serializable

class KeyBasedQueryException(query: DomainQuery, column: Column) 
    extends ScrayException(ExceptionIDs.keyBasedQueryExceptionID, query.getQueryID, s"""query trys to access column ${column.columnName} from queryspace
    ${query.getQueryspace} which has not been registered""") with Serializable

class NonAtomicClauseException(query: Query)
    extends ScrayException(ExceptionIDs.nonAtomicClauseExceptionID, query.getQueryID, "To qualify predicates those must be either be atomic clauses or 'And's")
    with Serializable

class NoPlanException(query: Query)
    extends ScrayException(ExceptionIDs.noPlanExceptionID, query.getQueryID, "Could not construct a plan from the query") with Serializable

class ExecutorShutdownException(query: Query)
    extends ScrayException(ExceptionIDs.plannerShutdownExceptionID, query.getQueryID, "The query engine has already been shut down. Cannot accept queries any more.") with Serializable
