package scray.common.serialization;

import java.io.Serializable;

/**
 * new BatchID for use with sync/api
 * @author stefan
 *
 */
public class BatchID implements Serializable {

	public BatchID(long startTime, long endTime) {
		batchStart = startTime;
		batchEnd = endTime;
	}
	
	private static final long serialVersionUID = -7082915998944716142L;

	private long batchStart;
	private long batchEnd;

	public long getBatchStart() {
		return batchStart;
	}

	public long getBatchEnd() {
		return batchEnd;
	}

	@Override
	public String toString() {
		return "BatchID" + batchStart + ":" + batchEnd;
	}
}
