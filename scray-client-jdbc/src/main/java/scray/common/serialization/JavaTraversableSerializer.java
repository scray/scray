package scray.common.serialization;

import java.util.Collection;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

abstract public class JavaTraversableSerializer<T, A extends Collection<T>> extends Serializer<A>{

	abstract public A newCollection();
	
	@Override
	public void write(Kryo kryo, Output output, A object) {
		output.writeInt(object.size(), true);
		for(T item: object) {
			kryo.writeClassAndObject(output, item);
			output.flush();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public A read(Kryo kryo, Input input, Class<A> type) {
		int size = input.readInt(true);
		A collection = newCollection();
		for(int i = 0; i < size; i ++) {
			collection.add((T)kryo.readClassAndObject(input));
		}
		return collection;
	}
}
