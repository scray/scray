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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * simple cient-side JAVA-based row implementation
 */
public class JavaCompositeRow implements JavaRow {

	private List<JavaRow> rows = new ArrayList<JavaRow>();

	public JavaCompositeRow() {
	}

	public JavaCompositeRow(List<JavaRow> rows) {
		setRows(rows);
	}

	public List<JavaRow> getRows() {
		return rows;
	}

	public synchronized void setRows(List<JavaRow> rows) {
		this.rows = rows;
	}

	public synchronized <T> T getColumnValue(JavaColumn column) {
		for(JavaRow row: rows) {
			T value = row.getColumnValue(column);
			if(value != null) {
				return value;
			}
		}
		return null;
	}

	public synchronized Set<JavaColumn> getAllColumns() {
		HashSet<JavaColumn> al = new HashSet<JavaColumn>();
		for (JavaRow row : rows) {
			al.addAll(row.getAllColumns());
		}
		return al;
	}
}
