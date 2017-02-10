package scray.common.serialization;

import java.util.Set;

import java.util.HashSet;

public class JavaSetSerializer<T> extends JavaTraversableSerializer<T, Set<T>>{

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Set<T> newCollection() {
		return (Set<T>) new HashSet();
	}
}
