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

/**
 * Column representation in JAVA
 */
public class JavaColumn {

	private String dbSystem = null;
	private String dbId = null;
	private String tableId = null;
	private String column = null;
	
	public JavaColumn() {}
	
	public JavaColumn(String dbSystem, String dbId, String tableId, String column) {
		this.dbSystem = dbSystem;
		this.dbId = dbId;
		this.dbSystem = dbSystem;
		this.column = column;
	}
	
	public String getDbSystem() {
		return dbSystem;
	}
	public void setDbSystem(String dbSystem) {
		this.dbSystem = dbSystem;
	}
	public String getDbId() {
		return dbId;
	}
	public void setDbId(String dbId) {
		this.dbId = dbId;
	}
	public String getTableId() {
		return tableId;
	}
	public void setTableId(String tableId) {
		this.tableId = tableId;
	}
	public String getColumn() {
		return column;
	}
	public void setColumn(String column) {
		this.column = column;
	}
}
