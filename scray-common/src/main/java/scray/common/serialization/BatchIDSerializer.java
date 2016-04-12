package scray.common.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Serializer for BatchIDs
 */
public class BatchIDSerializer extends Serializer<BatchID> {

	@Override
	public BatchID read(Kryo kryo, Input in, Class<BatchID> clazz) {
		return new BatchID(in.readLong(), in.readLong());
	}

	@Override
	public void write(Kryo kryo, Output output, BatchID batchID) {
		output.writeLong(batchID.getBatchStart());
		output.writeLong(batchID.getBatchEnd());		
	}

}
