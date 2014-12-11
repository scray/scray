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

namespace java scray.service.qmodel.thriftjava
#@namespace scala scray.service.qmodel.thrifscala

/**
 * Identifier
 */
struct ScrayUUID {
    1: i64 mostSigBits, 
    2: i64 leastSigBits
}

//////////////////////////////////////////////////////////////////////////////////
// schema/meta types
//////////////////////////////////////////////////////////////////////////////////

/**
 * Table identifier
 */
struct ScrayTTableInfo {
	1: string dbSystem,                // e.g. "cassandra"
	2: string dbId,					   // e.g. cassandra keyspace
	3: string tableId,                 // e.g. cassandra column family
	4: optional string keyT            // optional table key type (classname)
}

/**
 * Column identifier
 */
struct ScrayTColumnInfo {
	1: string name,							    // column name
	2: optional string columnT,	                // optional Column type (classname)
	3: optional ScrayTTableInfo tableId			// optional table identifier
}

/**
 * Query identifier
 */
struct ScrayTQueryInfo {
	1: optional ScrayUUID queryId,             // optional query id (set by planner)
	2: string querySpace,    			       // predefined query context
	3: ScrayTTableInfo tableInfo,			   // table identifier
    4: list<ScrayTColumnInfo> columns,		   // columns to fetch (in list order)
    5: optional i32 pagesize,                  // number of rows per result frame
    6: optional i64 expires                    // results won't be available after this (epoch) time
}

//////////////////////////////////////////////////////////////////////////////////
// query types
//////////////////////////////////////////////////////////////////////////////////
	
/**
 * Main query type
 */
struct ScrayTQuery {
	1: ScrayTQueryInfo queryInfo,   // query meta information
    2: map<string, binary> values,  // query expression named values
	3: string queryExpression	    // query expression string
}

//////////////////////////////////////////////////////////////////////////////////
// result types
//////////////////////////////////////////////////////////////////////////////////

struct ScrayTRow {
	1: optional binary rowId,          // key value
	2: optional list<binary> columns   // column list option, missing of which marks the end of the result set
}
