package scray.hdfs.io.configure;

import java.util.UUID;

public class RandomUUIDFilenameCreator implements FilenameCreator {

	@Override
	public String getNextFilename() {
		return UUID.randomUUID().toString();
	}
}
