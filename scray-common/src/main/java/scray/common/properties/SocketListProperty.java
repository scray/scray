package scray.common.properties;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.net.UnknownServiceException;
import java.util.HashSet;
import java.util.Set;

public class SocketListProperty extends Property<String, Set<InetSocketAddress>> {

	private String hostSeperator = ",";
	private String portSeperator = ":";
	
	private String name = null;
	private Set<InetSocketAddress> defaultValue = null;
	private int defaultPort = -1;

	public SocketListProperty(final String name, final int defaultPort) {
		this.name = name;
		this.defaultPort = defaultPort;
		super.addConstraint(new PropertyConstraint<String>() {
			public boolean checkConstraint(String value) {
				if(!(value instanceof String)) {
					return false;
				} else {
					
					try {
						String[] strs = value.split(hostSeperator);
						for(String str : strs) {
							String[] hostAndPort = str.trim().split(portSeperator);
							InetAddress.getByName(hostAndPort[0].trim());
							if(defaultPort <= 0 && hostAndPort.length < 2 || hostAndPort.length > 2) {
								throw new UnknownServiceException("<Host>:<Port> is required for configuration of host " + 
										str + " for porperty " + name);
							}
							if(hostAndPort.length == 2) {
								int port = new Integer(hostAndPort[1].trim());
								if(port <= 0 || port > 65535) {
									throw new UnknownServiceException("Port must be between 0 and 65535 in configuration of host " + 
											str + " for porperty " + name);
								}
							}
						}
					} catch(UnknownHostException uhe) {
						return false;
					} catch (UnknownServiceException e) {
						return false;
					}
				}
				return value instanceof String;
			}
		});
	}

	public SocketListProperty(String name) {
		this(name, -1);
	}
	
	public SocketListProperty(String name, int defaultPort, Set<InetSocketAddress> defaultValue) {
		this(name, defaultPort);
		this.defaultValue = defaultValue;
	}
	
	@Override
	public boolean hasDefault() {
		return defaultValue != null;
	}

	@Override
	public Set<InetSocketAddress> getDefault() {
		return defaultValue;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Set<InetSocketAddress> transformToResult(String value) {
		Set<InetSocketAddress> result = new HashSet<InetSocketAddress>();
		String[] strs = value.split(hostSeperator);
		for(String str : strs) {
			String[] hostAndPort = str.trim().split(portSeperator);
			if(hostAndPort.length == 2) {
				int port = new Integer(hostAndPort[1].trim());
				result.add(new InetSocketAddress(hostAndPort[0].trim(), port));
			} else {
				result.add(new InetSocketAddress(hostAndPort[0].trim(), defaultPort));
			}
		}
		return result;
	}
	
	@Override
	public String transformToStorageFormat(Set<InetSocketAddress> value) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for(InetSocketAddress addr: value) {
			if(!first) {
				sb.append(",");
			}
			sb.append(addr.getHostString());
			sb.append(":");
			sb.append(addr.getPort());
			first = false;
		}
		return sb.toString();
	}
	
	public void setSeperatorsString(String hostSeperator, String portSeperator) {
		this.hostSeperator = hostSeperator;
		this.portSeperator = portSeperator;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public boolean checkConstraintsOnValue(Object value) {
		if(value instanceof Set<?>) {
			for(Object obj: (Set<Object>)value) {
				if(!(obj instanceof InetSocketAddress)) {
					return false;
				} else {
					InetSocketAddress inetsocket = (InetSocketAddress)obj;
					if(inetsocket.getPort() < 1 || inetsocket.getPort() > 65535) {
						return false;
					}
				}
			}
			return true;
		}
		return false;
	}

}
