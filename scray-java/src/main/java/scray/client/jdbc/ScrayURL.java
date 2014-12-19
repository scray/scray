package scray.client.jdbc;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * URL schema for scray connections.
 * 
 * jdbc:scray://host:port/dbSystem/dbId/querySpace
 * 
 */
public class ScrayURL {

	public static final String SCHEME = "jdbc";
	public static final String SUBSCHEME = "scray";

	public static enum PathComponents {
		DBSYSTEM, DBID, QUERYSPACE
	}

	private Logger log = LoggerFactory.getLogger(ScrayURL.class);

	public URI opaque;

	public ScrayURL(String url) throws URISyntaxException {
		this.opaque = new URI(url);
	}

	// check and transform opaque jdbc url into absolute uri (by excluding the
	// sub schema)
	public URI transformOpaqueUri(URI opaqueUri) throws URISyntaxException {
		String schemeSpecificPart = opaqueUri.getSchemeSpecificPart();

		int startOfHier = schemeSpecificPart.indexOf(':');
		String first = schemeSpecificPart.substring(0, startOfHier);
		String rest = schemeSpecificPart.substring(startOfHier + 1);

		if (first.equals(SUBSCHEME)) {
			return new URI(opaqueUri.getScheme() + ":" + rest);
		} else {
			throw new URISyntaxException(first, "invalid sub-scheme");
		}
	}

	// Check hierarchical part of transformed jdbc url for required elements
	public boolean check() {
		try {
			URI hierUri = transformOpaqueUri(opaque);

			if (!hierUri.getScheme().equals(SCHEME)) {
				throw new URISyntaxException(hierUri.getScheme(),
						"faulty scheme");
			}
			if (hierUri.getHost() == null) {
				throw new URISyntaxException(hierUri.getHost(), "faulty host");
			}
			if (hierUri.getPort() == -1) {
				throw new URISyntaxException(String.valueOf(hierUri.getPort()),
						"faulty port");
			}
			StringTokenizer tokenizer = new StringTokenizer(hierUri.getPath(),
					"/");
			if (tokenizer.countTokens() != 3) {
				throw new URISyntaxException(hierUri.getPath(), "faulty path");
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

	public String getQuerySpace() {
		return getPathComponent(PathComponents.QUERYSPACE.ordinal());
	}

	public String getHostAndPort() {
		String token = null;
		try {
			URI hier = transformOpaqueUri(opaque);
			token = hier.getHost() + ":" + hier.getPort();
		} catch (URISyntaxException ex) {
			// eat
		}
		return token;
	}

	private String getPathComponent(int index) {
		String token = null;
		try {
			StringTokenizer tokenizer = new StringTokenizer(transformOpaqueUri(
					opaque).getPath(), "/");
			token = tokenizer.nextToken();
			for (int i = 0; i < index; i++) {
				token = tokenizer.nextToken();
			}
		} catch (URISyntaxException ex) {
			// eat
		}
		return token;
	}

}
