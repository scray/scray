package scray.hdfs.io.write;

enum WriteResultType {
	FAILURE, SUCCESS
}

interface WriteResult {
	public WriteResultType getWriteResultType();
}
