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
 * Stateful query service with continuous result pages
 */
service ScrayStatefulTService {

	/**
	 * Submit query
	 */
	scrayQModel.ScrayUUID query(1: scrayQModel.ScrayTQuery query) throws (1: ScrayTException ex)
	
	/**
	 * Fetch query results
	 * Paging state is maintained on server side.
	 * The operation is neither idempotent nor safe.
	 */
	ScrayTResultFrame getResults(1: scrayQModel.ScrayUUID queryId) throws (1: ScrayTException ex)

}

/**
 * Stateless query service with indexed result pages
 */
service ScrayStatelessTService {

    /**
     * Submit query
     */
    scrayQModel.ScrayUUID query(1: scrayQModel.ScrayTQuery query) throws (1: ScrayTException ex),
    
    /**
     * Fetch query result pages by index.
     * Paging state is to be maintained on client side.
     * The operation is idempotent and safe.
     */
    ScrayTResultFrame getResults(1: scrayQModel.ScrayUUID queryId, 2: i32 pageIndex) throws (1: ScrayTException ex)

}

/**
 * Scray service endpoint
 */
struct ScrayTServiceEndpoint {
	1: string host,									// hostname or IP
    2: i32 port,        							// port
    3: optional scrayQModel.ScrayUUID endpointId	// endpoint key
    4: optional i64 expires							// epoch expiration time
}

/**
 * Scray meta service
 * The service is provided by scray seed nodes
 */
service ScrayMetaTService {

    /**
     * Fetch a list of service endpoints.
     * Each endpoint provides ScrayStatelessTService and ScrayStatefulTService alternatives.
     * Queries can address different endpoints for load distribution.
     */
	list<ScrayTServiceEndpoint> getServiceEndpoints(),

	/**
	 * Add new service endpoint.
	 * The endpoint will be removed after a default expiration period.
	 */	
	ScrayTServiceEndpoint addServiceEndpoint(ScrayTServiceEndpoint endpoint),
	
	/**
	 * Restore the default expiration period of an endpoint.
	 */	
	void refreshServiceEndpoint(scrayQModel.ScrayUUID endpointID),

	/**
	 * Return vital sign
	 */
	bool ping(),

	/**
	 * Shutdown the server
	 */
	void shutdown(optional i64 waitNanos)

}

/**
 * Combined Scray service
 * Combines the ScrayMetaTService operations with the  ScrayStatefulTService operations
 */
service ScrayCombinedStatefulTService {

    /**
     * Fetch a list of service endpoints.
     * Each endpoint provides ScrayStatelessTService and ScrayStatefulTService alternatives.
     * Queries can address different endpoints for load distribution.
     */
	list<ScrayTServiceEndpoint> getServiceEndpoints(),

	/**
	 * Add new service endpoint.
	 * The endpoint will be removed after a default expiration period.
	 */	
	ScrayTServiceEndpoint addServiceEndpoint(ScrayTServiceEndpoint endpoint),
	
	/**
	 * Restore the default expiration period of an endpoint.
	 */	
	void refreshServiceEndpoint(scrayQModel.ScrayUUID endpointID),

	/**
	 * Return vital sign
	 */
	bool ping(),

	/**
	 * Shutdown the server
	 */
	void shutdown(optional i64 waitNanos)

	/**
	 * Submit query
	 */
	scrayQModel.ScrayUUID query(1: scrayQModel.ScrayTQuery query) throws (1: ScrayTException ex)
	
	/**
	 * Fetch query results
	 * Paging state is maintained on server side.
	 * The operation is neither idempotent nor safe.
	 */
	ScrayTResultFrame getResults(1: scrayQModel.ScrayUUID queryId) throws (1: ScrayTException ex)

}
