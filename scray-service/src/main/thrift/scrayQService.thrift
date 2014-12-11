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

// include query model
include "scrayQModel.thrift"
 
namespace java scray.service.qservice.thriftjava
#@namespace scala scray.service.qservice.thrifscala

/**
 * Scray-level exceptions
 */
exception ScrayTException {
	1: i32 what,
	2: string why
}

/**
 * Subset of result rows as transmission units
 */
struct ScrayTResultFrame {
	1: scrayQModel.ScrayTQueryInfo queryInfo,	// query meta information
	2: list<scrayQModel.ScrayTRow> rows			// list of rows
}

/**
 * Query executor with streaming result sets
 */
service ScrayTService {

	/**
	 * Submit queries
	 */
	scrayQModel.ScrayUUID query(1: scrayQModel.ScrayTQuery query) throws (1: ScrayTException ex)
	
	/**
	 * Fetch query results
	 */
	ScrayTResultFrame getResults(1: scrayQModel.ScrayUUID queryId) throws (1: ScrayTException ex)

}
