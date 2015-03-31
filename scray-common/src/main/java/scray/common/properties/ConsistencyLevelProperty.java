package scray.common.properties;

public class ConsistencyLevelProperty extends
		EnumProperty<ConsistencyLevelProperty.ConsistencyLevel> {

	public enum ConsistencyLevel {
		ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, LOCAL_ONE
	}

	public ConsistencyLevelProperty(String name, ConsistencyLevel defaultValue) {
		super(name, defaultValue, ConsistencyLevel.class);
	}

}
