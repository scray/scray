package scray

package object loader {

  object ExceptionIDs {
    // 600+ = query space registration errors
    val indexConfigExceptionID = "Scray-Index-Config-600"
    val propertySetExceptionID = "Scray-Set-Exception-601"
    val hostMissingExceptionID = "Scray-Config-Host-Missing-602"
    val urlMissingExceptionID = "Scray-Config-URL-Missing-603"
    val dbmsUndefinedExceptionID = "Scray-Config-DBMS-Missing-604"
    val mappingUnsupportedExceptionID = "Scray-Config-Mapping-Missing-605"
    val unknownTimeUnitExceptionID = "Scray-Config-Timeunit-Unknown-606"
  }
}