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
package scray.common.serialization.numbers;

/**
 * Repository for the numbers used with Kryo.
 * Java interoperability is ensured using JAVA.
 */
public enum KryoSerializerNumber {

	column(200), rowcolumn(201), simplerow(202), compositerow(203),	
	Set1(18), Set2(19), Set3(20), Set4(21), Set(22), // small scala sets -> java set
	UUID(81), // various JAVA type numbers
	BatchId(210); // because clients usually do not have a batch id 

	private int number;
	
	private KryoSerializerNumber(int number) {
		this.number = number;
	}
	
	public int getNumber() {
		return number; 
	}
}
