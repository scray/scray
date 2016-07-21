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
package scray.querying.description

/**
 * identifies a table
 */
case class TableIdentifier(dbSystem: String, dbId: String, tableId: String)

/**
 * represents a column
 */
case class Column(columnName: String, table: TableIdentifier)

/**
 * represents either all columns in a table (i.e. Left(true)) or a set of columns (i.e. Right(List(...)))
 */
case class Columns(columns: Either[Boolean, Set[Column]])

/**
 * represents ordering by a single column, e.g. order by in SQL terms with a single column
 */
case class ColumnOrdering[V](column: Column, descending: Boolean)(implicit val ordering: Ordering[V])

/**
 * represents grouping by a single column, e.g. group by in SQL terms with a single column
 */
case class ColumnGrouping(column: Column)

/**
 * represents skipping and limitting query fetching
 */
case class QueryRange(skip: Option[Long], limit: Option[Long], timeout: Option[Long])
