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
package scray.querying.caching

import scray.querying.queries.DomainQuery

class NullCache extends Cache[Nothing] {
  override def retrieve(query: DomainQuery): Option[Nothing] = None
  override def maintnance: Unit = {}
  override def close: Unit = {}
  override def report: MonitoringInfos = MonitoringInfos(0.0d, 0L, 0L, 0L)
}