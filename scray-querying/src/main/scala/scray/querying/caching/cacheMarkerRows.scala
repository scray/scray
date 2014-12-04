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

import scray.querying.description.EmptyRow

/**
 * marker rows denoting the end of a cache
 * serving action
 */
sealed trait CacheServeMarkerRow extends EmptyRow

/**
 * marker row that denotes cache ending stating 
 * that the cache served all rows 
 */
class CompleteCacheServeMarkerRow extends CacheServeMarkerRow

/**
 * marker row that denotes cache ending stating
 * that the cache served some rows, but there might be
 * more in the database; this is the default, if the 
 * all rows are returned and the cache is exhausted
 */
class IncompleteCacheServeMarkerRow extends CacheServeMarkerRow