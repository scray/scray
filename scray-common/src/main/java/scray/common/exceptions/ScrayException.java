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
package scray.common.serialization;

import java.util.UUID;
import java.io.Serializable;

/**
 * Common Scray Exception super class
 * @author andreas
 *
 */
public class ScrayException extends Exception implements Serializable {

	public ScrayException(String id, UUID query, String msg, Throwable cause) {
		super(id + ": " + msg + (q != null)?(" for query " + q):"", cause)
	}
	
	public ScrayException(String id, String msg, Throwable cause) {
		this(id, null, msg, cause)				
	}

	public ScrayException(String id, UUID query, String msg) {
		this(id, query, msg, null)		
	}

	public ScrayException(String id, String msg) {
		this(id, null, msg, null)
	}

}
