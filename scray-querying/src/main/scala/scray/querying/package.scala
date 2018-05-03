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
package scray

package object querying {
  
  object ExceptionIDs {
    // 000+ = query verification errors
    val queryWithoutColumnsExceptionID = "SIL-Scray-Querying-001"
    val queryspaceViolationExceptionID = "SIL-Scray-Querying-002"
    val queryspaceColumnViolationExceptionID = "SIL-Scray-Querying-003"
    val queryspaceViolationTableUnavailableExceptionID = "SIL-Scray-Querying-004"
    // 010+ = planner errors
    val queryDomainParserExceptionID = "SIL-Scray-Querying-010"
    val nonAtomicClauseExceptionID = "SIL-Scray-Querying-011"
    val noPlanExceptionID = "SIL-Scray-Querying-012"
    val plannerShutdownExceptionID = "SIL-Scray-Querying-013"
    val indexTypeExceptionID = "SIL-Scray-Querying-014"
    val queryCostsAreTooHigh = "SIL-Scray-Querying-015"
    val noQueryspaceRegistered = "SIL-Scray-Querying-016"
    // 700+ = caching errors
    val wrongQueryTypeForCacheID = "SIL-Scray-Querying-700"
    // 800+ = errors for specific queries
    val keyBasedQueryExceptionID = "SIL-Scray-Querying-800"
    val queryTimeOutExceptionID = "SIL-Scray-Querying-801"
    val unsupportedMaterializedViewID = "SIL-Scray-Querying-802"
    // 900+ = errors for specific indexes
    val queryDomainRangeException = "SIL-Scray-Querying-901"
    val queryWildcardRangeException = "SIL-Scray-Querying-902"
    val combinedIndexColumnMissingException = "SIL-Scray-Querying-903"
  }
}