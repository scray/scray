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
import java.util.Set;
import java.util.UUID;
import java.math.BigInteger;

import scray.common.serialization.numbers.KryoRowTypeNumber;
import scray.common.serialization.numbers.KryoSerializerNumber;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.twitter.chill.java.UUIDSerializer;

/**
 * some classes for JAVA-interoperability to prevent importing Scala dependencies.
 */
public class JavaKryoRowSerialization {

	public static void registerSerializers(Kryo kryo) {
		kryo.register(JavaColumn.class, new JavaColumnSerializer(), KryoSerializerNumber.column.getNumber());
		kryo.register(JavaRowColumn.class, new RowColumnSerializer(), KryoSerializerNumber.rowcolumn.getNumber());
		kryo.register(JavaSimpleRow.class, new JavaSimpleRowSerializer(), KryoSerializerNumber.simplerow.getNumber());
		kryo.register(JavaCompositeRow.class, new JavaCompositeRowSerializer(), KryoSerializerNumber.compositerow.getNumber());
		kryo.register(JavaBatchID.class, new BatchIDSerializer(), KryoSerializerNumber.BatchId.getNumber());
		kryo.register(Set.class, new JavaSetSerializer<>(), KryoSerializerNumber.Set1.getNumber());
		kryo.register(Set.class, new JavaSetSerializer<>(), KryoSerializerNumber.Set2.getNumber());
		kryo.register(Set.class, new JavaSetSerializer<>(), KryoSerializerNumber.Set3.getNumber());
		kryo.register(Set.class, new JavaSetSerializer<>(), KryoSerializerNumber.Set4.getNumber());
		kryo.register(Set.class, new JavaSetSerializer<>(), KryoSerializerNumber.Set.getNumber());
		kryo.register(BigInteger.class, new JavaBigIntegerSerializer(), KryoSerializerNumber.BigInteger.getNumber());
		kryo.register(UUID.class, new UUIDSerializer(), KryoSerializerNumber.UUID.getNumber());
	}
	
	/**
	 * kryo serializer for JavaColumn
	 */
	public static class JavaColumnSerializer extends Serializer<JavaColumn> {

		@Override
		public void write(Kryo k, Output o, JavaColumn v) {
		    o.writeString(v.getDbSystem());
		    o.writeString(v.getDbId());
		    o.writeString(v.getTableId());
		    o.writeString(v.getColumn());
		}

		@Override
		public JavaColumn read(Kryo k, Input i, Class<JavaColumn> type) {
		    String dbSystem = i.readString();
		    String dbId = i.readString();
		    String tableId = i.readString();
		    String column = i.readString();
			return new JavaColumn(dbSystem, dbId, tableId, column);
		}
	}
	
	/**
	 * kryo serializer for JavaRowColumn
	 */
	public static class RowColumnSerializer extends Serializer<JavaRowColumn<?>> {
	
		@Override
		public void write(Kryo k, Output o, JavaRowColumn<?> v) {
			k.writeObject(o, v.getColumn());
			k.writeClassAndObject(o, v.getValue());
		}

		@Override
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public JavaRowColumn<?> read(Kryo k, Input i, Class<JavaRowColumn<?>> type) {
			JavaColumn column = k.readObject(i, JavaColumn.class);
			return new JavaRowColumn(column, k.readClassAndObject(i));
		}
	}

	/**
	 * kryo serializer for JavaSimpleRow
	 */
	public static class JavaSimpleRowSerializer extends Serializer<JavaSimpleRow> {
	
		@Override
		public void write(Kryo k, Output o, JavaSimpleRow v) {
			o.writeShort(v.getColumns().size());
			for(JavaRowColumn<?> rowcol: v.getColumns()) {
				k.writeObject(o, rowcol);
			}
		}

		@Override
		public JavaSimpleRow read(Kryo k, Input i, Class<JavaSimpleRow> type) {
		    ArrayList<JavaRowColumn<?>> abuf = new ArrayList<JavaRowColumn<?>>();
			int number = i.readShort();
		    for(int j = 0; j < number; j++) {
		    	abuf.add(k.readObject(i, JavaRowColumn.class));
		    }
			return new JavaSimpleRow(abuf);
		}
	}

	/**
	 * kryo serializer for JavaSimpleRow
	 */
	public static class JavaCompositeRowSerializer extends Serializer<JavaCompositeRow> {
	
		@Override
		public void write(Kryo k, Output o, JavaCompositeRow v) {
			o.writeShort(v.getRows().size());
			for(JavaRow rowcol: v.getRows()) {
				if(rowcol instanceof JavaSimpleRow) {
					o.writeByte(KryoRowTypeNumber.simplerow.getNumber());
					k.writeObject(o, (JavaSimpleRow)rowcol);
				}
				if(rowcol instanceof JavaCompositeRow) {
					o.writeByte(KryoRowTypeNumber.compositerow.getNumber());
					k.writeObject(o, (JavaCompositeRow)rowcol);
				}
			}
		}

		@Override
		public JavaCompositeRow read(Kryo k, Input i, Class<JavaCompositeRow> type) {
		    ArrayList<JavaRow> abuf = new ArrayList<JavaRow>();
			int number = i.readShort();
		    for(int j = 0; j < number; j++) {
		    	int typ = i.readByte();
		    	if(typ == KryoRowTypeNumber.simplerow.getNumber()) {
		    		abuf.add(k.readObject(i, JavaSimpleRow.class));
		    	}
		    	if(typ == KryoRowTypeNumber.compositerow.getNumber()) {
		    		abuf.add(k.readObject(i, JavaCompositeRow.class));
		    	}
		    }
			return new JavaCompositeRow(abuf);
		}
	}
	
	/**
	 * kryo serializer for BigIntegers
	 */
	public static class JavaBigIntegerSerializer extends Serializer<BigInteger> {
		@Override
		public void write(Kryo k, Output o, BigInteger bi) {
			o.writeString(bi.toString());
		}

		@Override
		public BigInteger read(Kryo k, Input i, Class<BigInteger> type) {
			return new BigInteger(i.readString());
		}
	}

}
