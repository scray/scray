package scray.common.properties;

/**
 * Extends functionality of PropertyStorage with a storage operation
 * @author andreas
 *
 */
public interface PropertyStoragePuttable extends PropertyStorage {

	/**
	 * set a property value on the storage device.
	 * Calling the property change listener is left to the implementation.  
	 * @param name
	 * @param value
	 */
	public <T, U> void put(Property<T, U> name, T value);
	
	/**
	 * This method initializes the storage service and is called before
	 * any calls to get and put.
	 */
	public void init();
}
