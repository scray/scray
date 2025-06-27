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

import scray.querying.description.TableIdentifier
import scray.querying.description.Column
import scray.querying.description.TableConfiguration
import scray.querying.queries.DomainQuery
import scray.common.key.api.KeyGenerator
import scray.common.key.OrderedStringKeyGenerator

/**
 * Contains information to find a materialized view
 * 
 * @param krimaryKey Primary key column in database
 */
case class MaterializedView(
    val table:TableIdentifier,
    val keyGenerationClass: KeyGenerator[Array[String]] = OrderedStringKeyGenerator,
    val primaryKeyColumn: String = "key"
    //val fixedDomains: Array[Column]//, // single value domains -> multiple possible values 
    //rangeDomains: Array[(Column, Array[RangeValueDomain[_]])], // range value domains -> 
    //viewTable: TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _], // table implementing this materialized view
    //checkMaterializedView: (MaterializedView, DomainQuery) => Option[(Boolean, Int)]
) 

