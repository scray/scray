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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * simple cient-side JAVA-based row implementation
 */
public class JavaSimpleRow implements JavaRow {

	private List<JavaRowColumn<?>> columns = new ArrayList<JavaRowColumn<?>>();

	transient private HashMap<JavaColumn, JavaRowColumn<?>> colHash = new HashMap<JavaColumn, JavaRowColumn<?>>();

	public JavaSimpleRow() {
	}

	public JavaSimpleRow(List<JavaRowColumn<?>> columns) {
		setColumns(columns);
	}

	public List<JavaRowColumn<?>> getColumns() {
		return columns;
	}

	public synchronized void setColumns(List<JavaRowColumn<?>> columns) {
		this.columns = columns;
		for(JavaRowColumn<?> rowcol: columns) {
			colHash.put(rowcol.getColumn(), rowcol);
		}
	}

	@SuppressWarnings("unchecked")
	public synchronized <T> T getColumnValue(JavaColumn column) {
		if (columns != null && columns.size() == colHash.size()) {
			JavaRowColumn<T> value = (JavaRowColumn<T>) colHash.get(column);
			if (value != null) {
				return value.getValue();
			}
		} else {
			for (JavaRowColumn<?> rowcol : columns) {
				if (column.getColumn().equals(rowcol.getColumn().getColumn())
						&& column.getTableId().equals(
								rowcol.getColumn().getTableId())
						&& column.getDbSystem().equals(
								rowcol.getColumn().getDbSystem())
						&& column.getDbId()
								.equals(rowcol.getColumn().getDbId())) {
					return (T) rowcol.getValue();
				}
			}
		}
		return null;
	}

	public synchronized Set<JavaColumn> getAllColumns() {
		if (columns != null && columns.size() == colHash.size()) {
			return colHash.keySet();
		}
		HashSet<JavaColumn> al = new HashSet<JavaColumn>();
		for (JavaRowColumn<?> rowcol : columns) {
			al.add(rowcol.getColumn());
		}
		return al;
	}
}
