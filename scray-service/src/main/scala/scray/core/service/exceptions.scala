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

package scray.core.service

import java.util.UUID
import scray.common.exceptions.ScrayException

object ExceptionIDs {
  val PARSING_ERROR = "SIL-Scray-Service-010-Parsing"
  val SPOOLING_ERROR = "SIL-Scray-Service-011-Spooling"
  val CACHING_ERROR = "SIL-Scray-Service-012-Cache"
}

class ScrayServiceException(id : String, query : UUID, msg : String, cause : Throwable)
  extends ScrayException(id, query, msg, cause) {
  def this(id : String, msg : String, cause : Throwable) = this(id, null, msg, cause)
  def this(id : String, query : UUID, msg : String) = this(id, query, msg, null)
  def this(id : String, msg : String) = this(id, null, msg, null)
}
