package scray.hdfs.io.configure;


/**
 * Interface to get the file name for the next file
 * @author Stefan Obermeier
 */
public interface FilenameCreator {
	/**
	 * 
	 * @return filename for next file.
	 */
	public String getNextFilename();
}
