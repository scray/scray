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
	
	@Override
	public boolean equals(Object o) {
	    if (this == o)
	        return true;
	   
	    if (o == null)
	        return false;
	   
	    
	    if (getClass() != o.getClass())
	        return false;
	    FixNameCreator otherCreator = (FixNameCreator) o;

	    return this.filename.equals(otherCreator.filename);
	}

}
