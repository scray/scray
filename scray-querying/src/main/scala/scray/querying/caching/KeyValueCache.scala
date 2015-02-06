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
import scray.querying.source.LazyDataFuture
import org.mapdb._
import scray.querying.queries.KeyBasedQuery
import com.twitter.concurrent.Spool
import scray.querying.description.internal.WrongQueryTypeForCacheException

class KeyValueCache[K, V](
    val sourceDiscriminant: String,
    val valueSerializer: Option[Serializer[V]] = None,
    val cachesizegb: Double = 1.0D,
    val numberentries: Option[Int] = None) extends Cache[V] {

  val db = DBMaker.newMemoryDirectDB().transactionDisable().asyncWriteEnable().make
  val cache = db.createHashMap("cache").counterEnable().expireStoreSize(cachesizegb).
                  valueSerializer(valueSerializer.orNull).make[K, V]
  val store = Store.forDB(db)

  /**
   * retrieve one row, ending is an implicit of existing contents (i.e. can only be one)
   */
  override def retrieve(query: DomainQuery): Option[V] = query match {
    case keyquery: KeyBasedQuery[K] => Option(cache.get(keyquery.key))
    case _ => throw new WrongQueryTypeForCacheException(query, sourceDiscriminant)
  }

  def put(query: DomainQuery, value: V) = query match {
    case keyquery: KeyBasedQuery[K] => cache.put(keyquery.key, value)
    case _ => throw new WrongQueryTypeForCacheException(query, sourceDiscriminant)
  }

  override def maintnance: Unit = {
    // expiring old columns can be handled automatically by MapDB in this case
  }

  override def close: Unit = {
    cache.close
  }

  override def report: MonitoringInfos = MonitoringInfos(cachesizegb, cache.sizeLong, store.getCurrSize, store.getFreeSize)
}