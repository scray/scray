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

import java.util.Set;

/**
 * Row which contains accessors for the columns
 */
public interface JavaRow {
	
	/**
	 * get a value of this row
	 */
	public <T> T getColumnValue(JavaColumn column);
	
	/**
	 * get all columns of this Row
	 */
	public Set<JavaColumn> getAllColumns(); 
}
