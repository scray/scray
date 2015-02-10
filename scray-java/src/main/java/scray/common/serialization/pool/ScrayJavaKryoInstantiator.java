package scray.common.serialization.pool;

import scray.common.serialization.JavaKryoRowSerialization;
import scray.common.serialization.pool.KryoJavaPoolSerialization.SerializerEntry;

import com.esotericsoftware.kryo.Kryo;
import com.twitter.chill.KryoInstantiator;

public class ScrayJavaKryoInstantiator extends KryoInstantiator {

	private static final long serialVersionUID = 1L;

	@Override
	public Kryo newKryo() {
		Kryo k = super.newKryo();
		k.setRegistrationRequired(false);
		JavaKryoRowSerialization.registerSerializers(k);
		
		for (SerializerEntry<?> ser : KryoJavaPoolSerialization.getInstance()
				.getSerializers()) {
			k.register(ser.cls, ser.ser, ser.num);
		}
		return k;
	}

}
