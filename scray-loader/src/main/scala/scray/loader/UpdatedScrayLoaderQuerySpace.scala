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
package scray.loader

import scray.querying.description.QueryspaceConfiguration
import scray.querying.description.TableConfiguration
import scray.querying.description.internal.MaterializedView
import scray.querying.queries.DomainQuery
import scray.querying.description.ColumnConfiguration
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.loader.configparser.ScrayQueryspaceConfiguration
import scray.loader.configparser.ScrayConfiguration
import scray.loader.configuration.ScrayStores
import scala.collection.mutable.HashMap
import scray.querying.storeabstraction.StoreGenerators
import scray.querying.sync.DbSession
import scray.querying.description.ColumnConfiguration
import scray.querying.description.TableIdentifier
import scray.querying.storeabstraction.StoreExtractor
import scray.querying.description.VersioningConfiguration
import scray.querying.description.Row
import scray.querying.description.Column
import scray.querying.source.store.QueryableStoreSource
import com.twitter.util.FuturePool
import scray.querying.description.IndexConfiguration
import scray.querying.Registry
import scray.common.key.OrderedStringKeyGenerator
import scray.common.key.OrderedStringKeyGenerator
import scray.common.key.StringKey
import scray.common.errorhandling.ErrorHandler
import scray.common.errorhandling.ScrayProcessableErrors
import scray.common.errorhandling.LoadCycle


/**
 * a query space that can be used to contain re-loaded tables from
 * various different databases. Used by the update method of ScrayLoaderQuerySpace 
 */
class UpdatedScrayLoaderQuerySpace(name: String, config: ScrayConfiguration, override val qsConfig: ScrayQueryspaceConfiguration,
    storeConfig: ScrayStores, futurePool: FuturePool, 
    tables: Set[TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]],
    columns: List[ColumnConfiguration], errorHandler: ErrorHandler)
    extends ScrayLoaderQuerySpace(name, config, qsConfig, storeConfig, futurePool, errorHandler) with LazyLogging {
  

  override def getTables(version: Int): Set[TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]] = tables
  
  override def getColumns(version: Int): List[ColumnConfiguration] = columns
}


