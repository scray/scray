package scray.sync.impl;

import java.util.Map;
import java.util.Optional;

import scray.sync.api.VersionedData;

public interface Idx {

	public Map<Integer, Map<Integer, VersionedData>> put(Optional<VersionedData> oldV, VersionedData newV, Map<Integer, Map<Integer, VersionedData>> versionInformationsIdxEnvState);
	public Optional<Integer> getKey(String attribute1, String attribute2);
}
