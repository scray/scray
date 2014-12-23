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

package scray.core

import java.util.UUID
import scray.service.qmodel.thrifscala.ScrayUUID
package object service {

  // scray server endpoint
  val ENDPOINT = "localhost:18181"

  // memcached host
  val MEMCACHED_HOST = "localhost:11211"

  implicit def UUID2ScrayUUID(uuid : UUID) : ScrayUUID =
    ScrayUUID(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())

  implicit def ScrayUUID2UUID(suuid : ScrayUUID) : UUID =
    new UUID(suuid.leastSigBits, suuid.mostSigBits)

}
