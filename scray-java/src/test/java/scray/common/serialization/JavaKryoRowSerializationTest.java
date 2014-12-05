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

import java.io.InputStream;

import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import static org.junit.Assert.*;

/**
 * Test interoperability with Scala result-set-classes
 */
public class JavaKryoRowSerializationTest {

	@Test
	public void testDesrializeScalaSerializedStuff() {
		// this is a scala-generated binary file with a serialized row.
		// It should contain 2 columns and respective values
		Kryo k = new Kryo();
		JavaKryoRowSerialization.registerSerializers(k);
		InputStream file1 = this.getClass().getResourceAsStream("/serializations/scraytest2.txt");
	    Input input = new Input(file1);
	    JavaSimpleRow result = k.readObject(input, JavaSimpleRow.class);
	    assertTrue(result instanceof JavaSimpleRow);
	    assertTrue(result.getColumns().size() == 2);
	    assertEquals(result.getColumns().get(0).getValue(), 1);
	    assertEquals(result.getColumns().get(1).getValue(), "blubb");
	}
}
