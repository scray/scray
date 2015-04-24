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
    // 700+ = caching errors
    val wrongQueryTypeForCacheID = "SIL-Scray-Querying-700"
    // 800+ = errors for specific queries
    val keyBasedQueryExceptionID = "SIL-Scray-Querying-800"
    // 900+ = errors for specific indexes
    val queryDomainRangeException = "SIL-Scray-Querying-901"
    val queryWildcardRangeException = "SIL-Scray-Querying-902"
  }
}