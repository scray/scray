/*
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// include base types
include "scrayBase.thrift"
 
namespace java scray.service.qmodel.thriftjava
#@namespace scala scray.service.qmodel.thrifscala

//////////////////////////////////////////////////////////////////////////////////
// schema/meta types
//////////////////////////////////////////////////////////////////////////////////

/**
 * Table identifier
 */
struct STableInfo {
	1: string dbSystem,				// e.g. "cassandra"
	2: string dbId,					// e.g. cassandra keyspace
	3: string tableId,				// e.g. cassandra column family
	4: scrayBase.STypeInfo keyT		// table key type
}

/**
 * Column identifier
 */
struct SColumnInfo {
	1: string name,							// column name
	2: optional scrayBase.STypeInfo sType,	// optional Column type
	3: optional STableInfo tableId			// optional table identifier
}

/**
 * Query identifier
 */
struct SQueryInfo {
	1: optional scrayBase.UUID queryId,	// optional query id (set by planner)
	2: string querySpace,    			// predefined query context
	3: STableInfo tableInfo,			// table identifier
    4: set<SColumnInfo> columns,		// Columns to fetch
}

//////////////////////////////////////////////////////////////////////////////////
// query types
//////////////////////////////////////////////////////////////////////////////////
	

/**
 * Main query type
 */
struct SQuery {
	1: SQueryInfo queryInfo,					// query meta information
    2: map<string, scrayBase.SValue> values,	// query expression named values
	3: string queryExpression					// query expression string
}

//////////////////////////////////////////////////////////////////////////////////
// result types
//////////////////////////////////////////////////////////////////////////////////

struct SRow {
	1: binary key,					// key value
	2: map<string, binary> columns	// mapped column values
}
