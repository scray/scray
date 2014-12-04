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
 
namespace java scray.service.base.thriftjava
#@namespace scala scray.service.base.thrifscala

/*
 * Atomic types
 */
enum ScrayTType {
 ANY = 0,
 BOOL = 1,
 INT = 2,
 LONG = 3,
 FLOAT = 4,
 STRING = 5,
 OBJECT = 6
 }

/**
 * Data type information
 */
struct ScrayTTypeInfo {
	1: ScrayTType tType,
	2: optional string className
}

/**
 * Data unit
 */
struct ScrayTValue {
	1: ScrayTTypeInfo sType,
	2: binary value
}

/**
 * Identifier
 */
struct ScrayUUID {
	1: i64 mostSigBits, 
	2: i64 leastSigBits
}
