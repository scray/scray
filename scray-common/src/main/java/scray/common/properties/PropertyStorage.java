package scray.common.properties;

/**
 * Storage abstraction for properties
 * @author andreas
 *
 */
public interface PropertyStorage {

	/**
	 * retrieve proerty value from storage service.
	 * This interface does not need to pay attention for 
	 * @param name the property to retrieve
	 * @return the value of the property or null, if it doesn't exist
	 */
	public <T, U> T get(Property<T, U> name);
	
	/**
	 * This method initializes the storage service and is called before
	 * any calls to get and put.
	 */
	public void init();
}
