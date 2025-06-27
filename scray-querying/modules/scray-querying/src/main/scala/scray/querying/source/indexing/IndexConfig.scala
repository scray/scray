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
package scray.querying.source.indexing

import java.util.TimeZone
import scray.querying.description.{Column, Row}

/**
 * all indexes that are allowed must have a configuration
 * that inherits from this
 */
sealed class IndexConfig(val indexReferencesColumn: Column) extends Serializable

/**
 * configuration for TimeIndexSource
 */
case class TimeIndexConfig(
    // column in the lookup-table that can be queried
    timeReferenceCol: Column,
    // column in the index-table that reflects the year for the time
    indexRowColumnYear: Column,
    // column in the index-table that contains the time information in ms
    indexColumnMs: Column,
    // column in the index-table that contains the references into the lookup-source
    override val indexReferencesColumn: Column,
    // if this index can be queried in parallel and how much parallelization is available
//    parallelization: Option[(QueryableStore[_ , _]) => Option[Int]] = None,
    // which column is used for parallelization
    parallelizationColumn: Option[Column] = None,
    // if this index is ordered
    ordering: Option[(Row, Row) => Boolean] = None,
    // time zone of index, only relevant for turns of the year
    timeZone: TimeZone = TimeZone.getTimeZone("UTC"), 
    // minimum year of data that is indexed
    minimumYear: Int = 2015,
    // query limit
    maxLimit: Option[Long] = Some(5000)) extends IndexConfig(indexReferencesColumn)

/**
 * configuration for WildcardIndexSource
 */
case class WildcardIndexConfig(
    // column in the lookup-table that can be queried
    referenceCol: Column,
    // first letters to be indexed
    indexRowNameCol: Column,
    // UTF8-Column for the name which is indexed
    indexNameColumn: Column,
    // column in the index-table that contains the references into the lookup-source
    override val indexReferencesColumn: Column,
    // the number of characters in the prefix
    numberOfPrefixCharacters: Int,
    // if the index is lower case, i.e. enables
    lowerCaseIndex: Boolean,
    // if the prefix is present in the data
    prefixPresent: Boolean,
    // query limit
    maxLimit: Option[Long] = Some(5000)) extends IndexConfig(indexReferencesColumn)    

/**
 * simple in-stream index
 */
case class SimpleHashJoinConfig(override val indexReferencesColumn: Column) extends IndexConfig(indexReferencesColumn)