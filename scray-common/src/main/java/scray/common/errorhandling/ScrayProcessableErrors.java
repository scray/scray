package scray.common.errorhandling;

public enum ScrayProcessableErrors {
	QS_NAME_CHANGED("scray-loader.queryspace.name.changed"),
	QS_VERSION_DECREASED("scray-loader.queryspace.version.decreased"),
	QS_COLUMN_FOUND("scray-loader.queryspace.column.newfound"),
	QS_COLUMN_REMOVED("scray-loader.queryspace.column.removed"),
	QS_DBMS_MISSING("scray-loader.queryspace.dbms.missing"),
	QS_DBID_MISSING("scray-loader.queryspace.dbid.missing"),
	QS_TABLE_FOUND("scray-loader.queryspace.table.newfound"),
	QS_TABLE_MISSING("scray-loader.queryspace.table.missing"),
	QS_MV_FOUND("scray-loader.queryspace.materializedview.newfound"),
	QS_MV_REMOVED("scray-loader.queryspace.materializedview.removed"),
	OSGI_BUNDLE_FAILED("scray-loader.osgi.bundle.startfailed"),
	OSGI_SERVICE_FAILED("scray-loader.osgi.service.openfailed"),
	CLASSLOADER_LOAD_FAILED("scray-loader.classloader.load.failed");
	
	private String identifier = null;
	private ScrayProcessableErrors(String identifier) {
			this.identifier = identifier;
	}

	public String getIdentifier() {
		return identifier;
	}
}
