package scray.hdfs.io.configure;

public class FixNameCreator implements FilenameCreator {
	private String filename;
	
	public FixNameCreator(String filename) {
		this.filename = filename;
	}
	
	@Override
	public String getNextFilename() {
		return this.filename; 
	}

}
