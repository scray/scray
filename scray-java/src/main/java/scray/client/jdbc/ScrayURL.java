package scray.client.jdbc;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * URL schema for scray connections.
 * 
 * scray://host:port/dbSystem/dbId/tableId/querySpace
 * 
 */
public class ScrayURL {

	public static final String SCHEME = "scray";

	public static enum PathComponents {
		DBSYSTEM, DBID, TABLEID, QUERYSPACE
	}

	private Logger log = LoggerFactory.getLogger(ScrayURL.class);

	public URI uri;

	public ScrayURL(String url) throws URISyntaxException {
		this.uri = new URI(url);
	}

	public boolean checkSyntax() {
		try {
			if (!uri.isAbsolute()) {
				throw new URISyntaxException(uri.toString(),
						"Faulty structure.");
			}
			if (!uri.getScheme().equals(SCHEME)) {
				throw new URISyntaxException(uri.getScheme(), "faulty scheme");
			}
			if (uri.getHost() == null) {
				throw new URISyntaxException(uri.getHost(), "faulty host");
			}
			if (uri.getPort() == -1) {
				throw new URISyntaxException(String.valueOf(uri.getPort()),
						"faulty port");
			}
			StringTokenizer tokenizer = new StringTokenizer(uri.getPath(), "/");
			if (tokenizer.countTokens() != 4) {
				throw new URISyntaxException(uri.getPath(), "faulty path");
			}
			return true;
		} catch (URISyntaxException e) {
			log.error("URI check failed: " + e.getMessage());
			return false;
		}
	}

	public String getDbSystem() {
		return getPathComponent(PathComponents.DBSYSTEM.ordinal());
	}

	public String getDbId() {
		return getPathComponent(PathComponents.DBID.ordinal());
	}

	public String getTableId() {
		return getPathComponent(PathComponents.TABLEID.ordinal());
	}

	public String getQuerySpace() {
		return getPathComponent(PathComponents.QUERYSPACE.ordinal());
	}

	public String getHostAndPort() {
		return uri.getHost() + ":" + uri.getPort();
	}

	private String getPathComponent(int index) {
		StringTokenizer tokenizer = new StringTokenizer(uri.getPath(), "/");
		String token = tokenizer.nextToken();
		for (int i = 0; i < index; i++) {
			token = tokenizer.nextToken();
		}
		return token;
	}

}
