package org.scray.sync.rest;

import scray.sync.api.VersionedData;
import scray.sync.api.VersionedDataApi;

public interface SyncEventManager {

	public void publishUpdate(VersionedData updatedVersionedData);
}
