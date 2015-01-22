package scray.client.jdbc;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

/**
 * URL schema for scray connections.
 * 
 * jdbc:scray:stateless://host:port/dbSystem/dbId/querySpace
 * jdbc:scray:stateful://host:port/dbSystem/dbId/querySpace
 * 
 */
public class ScrayURL {

	public static final String SCHEME = "jdbc";
	public static final String SUBSCHEME = "scray";

	public static enum ProtocolModes {
		stateful, stateless
	}

	public URI opaque;
	public UriPattern pattern;

	public ScrayURL(String url) throws URISyntaxException {
		this.opaque = new URI(url);
		this.pattern = new UriPattern(opaque);
	}

	public class UriPattern {

		final static String HIER_DELIM = "://";

		private URI absoluteUri;
		private String fullScheme;
		private String mainScheme;
		private String schemeExtension;
		private String subScheme;
		private String hierPart;
		private String protocolMode;
		private String host;
		private int port;
		private String hostAndPort;
		private String path;
		private String dbSystem;
		private String dbId;
		private String querySpace;

		public UriPattern(URI opaqueUri) throws URISyntaxException {

			/* extract absolute URI */

			mainScheme = opaqueUri.getScheme();

			if (!mainScheme.equals(SCHEME)) {
				throw new URISyntaxException(mainScheme,
						"Invalid URL: faulty main scheme");
			}

			String schemeSpecificPart = opaqueUri.getSchemeSpecificPart();
			int startOfHier = schemeSpecificPart.indexOf(HIER_DELIM);

			if (startOfHier < 0) {
				throw new URISyntaxException(opaqueUri.toString(),
						"Invalid URL: hierarchical part mismatch");
			}

			hierPart = schemeSpecificPart.substring(startOfHier);
			absoluteUri = new URI(mainScheme + hierPart);

			/* extract host and port */

			host = absoluteUri.getHost();

			if (host == null) {
				throw new URISyntaxException(host, "Invalid URL: faulty host");
			}

			port = absoluteUri.getPort();

			if (port == -1) {
				throw new URISyntaxException(String.valueOf(port),
						"Invalid URL: faulty port");
			}

			hostAndPort = host + ":" + String.valueOf(port);

			/* decompose path */

			path = absoluteUri.getPath();

			StringTokenizer pathElems = new StringTokenizer(path, "/");

			if (pathElems.countTokens() != 3) {
				throw new URISyntaxException(path, "Invalid URL: faulty path");
			}

			dbSystem = pathElems.nextToken();
			dbId = pathElems.nextToken();
			querySpace = pathElems.nextToken();

			/* decompose extended scheme */

			schemeExtension = schemeSpecificPart.substring(0, startOfHier);
			fullScheme = mainScheme + ":" + schemeExtension;

			StringTokenizer schemeExTokens = new StringTokenizer(
					schemeExtension, ":");

			if (schemeExTokens.countTokens() != 2)
				throw new URISyntaxException(fullScheme,
						"Invalid URL: scheme mismatch (must contain three parts)");

			subScheme = schemeExTokens.nextToken();

			if (!subScheme.equals(SUBSCHEME)) {
				throw new URISyntaxException(subScheme,
						"Invalid URL: faulty subScheme");
			}

			protocolMode = schemeExTokens.nextToken();

			if (!(protocolMode.equals(ProtocolModes.stateful.name()) || protocolMode
					.equals(ProtocolModes.stateless.name()))) {
				throw new URISyntaxException(protocolMode,
						"Invalid URL: faulty protocolMode");
			}
		}

		public String getMainScheme() {
			return mainScheme;
		}

		public String getSubScheme() {
			return subScheme;
		}

		public String getProtocolMode() {
			return protocolMode;
		}

		public String getHost() {
			return host;
		}

		public int getPort() {
			return port;
		}

		public String getHostAndPort() {
			return hostAndPort;
		}

		public String getDbSystem() {
			return dbSystem;
		}

		public String getDbId() {
			return dbId;
		}

		public String getQuerySpace() {
			return querySpace;
		}
	}

	public String getMainScheme() {
		return pattern.getMainScheme();
	}

	public String getSubScheme() {
		return pattern.getSubScheme();
	}

	public String getProtocolMode() {
		return pattern.getProtocolMode();
	}

	public String getHost() {
		return pattern.getHost();
	}

	public int getPort() {
		return pattern.getPort();
	}

	public String getHostAndPort() {
		return pattern.getHostAndPort();
	}

	public String getDbSystem() {
		return pattern.getDbSystem();
	}

	public String getDbId() {
		return pattern.getDbId();
	}

	public String getQuerySpace() {
		return pattern.getQuerySpace();
	}

	@Override
	public String toString() {
		return "ScrayURL [getMainScheme()=" + getMainScheme()
				+ ", getSubScheme()=" + getSubScheme() + ", getProtocolMode()="
				+ getProtocolMode() + ", getHost()=" + getHost()
				+ ", getPort()=" + getPort() + ", getHostAndPort()="
				+ getHostAndPort() + ", getDbSystem()=" + getDbSystem()
				+ ", getDbId()=" + getDbId() + ", getQuerySpace()="
				+ getQuerySpace() + "]";
	}
	
}
