package scray.common.properties;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.net.UnknownServiceException;
import java.util.ArrayList;
import java.util.List;

public class SocketListProperty extends Property<String, List<InetSocketAddress>> {

	private String hostSeperator = ",";
	private String portSeperator = ":";
	
	private String name = null;
	private List<InetSocketAddress> defaultValue = null;
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
	
	public SocketListProperty(String name, int defaultPort, List<InetSocketAddress> defaultValue) {
		this(name, defaultPort);
		this.defaultValue = defaultValue;
	}
	
	@Override
	public boolean hasDefault() {
		return defaultValue != null;
	}

	@Override
	public List<InetSocketAddress> getDefault() {
		return defaultValue;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public List<InetSocketAddress> transformToResult(String value) {
		List<InetSocketAddress> result = new ArrayList<InetSocketAddress>();
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
	
	public void setSeperatorsString(String hostSeperator, String portSeperator) {
		this.hostSeperator = hostSeperator;
		this.portSeperator = portSeperator;
	}
}
