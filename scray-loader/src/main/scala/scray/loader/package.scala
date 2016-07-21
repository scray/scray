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

package object loader {

  val VERSION = "0.9.9"
  
  object ExceptionIDs {
    // 600+ = query space registration errors
    val indexConfigExceptionID = "Scray-Index-Config-600"
    val propertySetExceptionID = "Scray-Set-Exception-601"
    val hostMissingExceptionID = "Scray-Config-Host-Missing-602"
    val urlMissingExceptionID = "Scray-Config-URL-Missing-603"
    val dbmsUndefinedExceptionID = "Scray-Config-DBMS-Missing-604"
    val mappingUnsupportedExceptionID = "Scray-Config-Mapping-Missing-605"
    val unknownTimeUnitExceptionID = "Scray-Config-Timeunit-Unknown-606"
    val unknownBooleanValueExceptionID = "Scray-Config-Boolean-Unknown-607"
  }  
}
