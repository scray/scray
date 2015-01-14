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
package scray.common.exceptions;

/**
 * Some generic Exception ids
 * @author andreas
 *
 */
public enum ExceptionIDs {

	// SIL-Scray-Commons-000+ error types
	GENERAL_FAULT("SIL-Scray-Commons-001-General"),
	SERIALIZATION_FAULT("SIL-Scray-Commons-002-Serialization"),
	UNIMPLEMNTED("SIL-Scray-Commons-003-Unimplemented");
	
	private ExceptionIDs(String name) {
		this.name = name;  
	}
	
	private String name;
	
	public String getName() {
		return name;
	}
}
