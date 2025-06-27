package org.scray.sync.rest;

import scray.sync.api.VersionedData;

public interface SyncEventManager {

	public void publishUpdate(VersionedData updatedVersionedData);
}
