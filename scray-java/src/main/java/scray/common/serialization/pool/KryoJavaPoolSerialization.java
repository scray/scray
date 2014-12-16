package scray.common.serialization.pool;

import java.util.LinkedList;
import java.util.List;

import com.esotericsoftware.kryo.Serializer;
import com.twitter.chill.KryoPool;

public class KryoJavaPoolSerialization {

	private KryoJavaPoolSerialization() {
	}

	private static class KryoJavaPoolSerializationHolder {
		private static KryoJavaPoolSerialization instance = new KryoJavaPoolSerialization();
	}

	public static KryoJavaPoolSerialization getInstance() {
		return KryoJavaPoolSerializationHolder.instance;
	}

	private int POOL_SIZE = 10;
	private ScrayJavaKryoInstantiator instantiator = new ScrayJavaKryoInstantiator();
	private List<SerializerEntry<?>> serializers = new LinkedList<SerializerEntry<?>>();

	public KryoPool chill = KryoPool.withByteArrayOutputStream(POOL_SIZE,
			instantiator);

	public List<SerializerEntry<?>> getSerializers() {
		return serializers;
	}

	public <T> void register(Class<T> cls, Serializer<T> serializer, int number) {
		serializers.add(new SerializerEntry<T>(cls, serializer, number));
	}

	class SerializerEntry<T> {
		Class<T> cls;
		Serializer<T> ser;
		int num;

		public SerializerEntry(Class<T> cls, Serializer<T> ser, int num) {
			this.cls = cls;
			this.ser = ser;
			this.num = num;
		}
	}

}
