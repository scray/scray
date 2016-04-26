package scray.common.properties;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

public class HostListProperty extends Property<String, Set<InetAddress>> {

	private String separator = ",";
	
	private String name = null;
	private Set<InetAddress> defaultValue = null;

	public HostListProperty(String name) {
		this.name = name;
		super.addConstraint(new PropertyConstraint<String>() {
			public boolean checkConstraint(String value) {
				System.out.println("//////////////////" + value);
				if(!(value instanceof String)) {
					return false;
				} else {
					try {
						String[] strs = value.split(separator);
						for(String str : strs) {
							InetAddress.getByName(str);
						}
					} catch(UnknownHostException uhe) {
						return false;
					}
				}
				return value instanceof String;
			}
		});
	}

	public HostListProperty(String name, Set<InetAddress> defaultValue) {
		this(name);
		this.defaultValue = defaultValue;
	}
	
	@Override
	public boolean hasDefault() {
		return defaultValue != null;
	}

	@Override
	public Set<InetAddress> getDefault() {
		return defaultValue;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Set<InetAddress> transformToResult(String value) {
		Set<InetAddress> result = new HashSet<InetAddress>();
		try {
			String[] strs = value.split(separator);
			for(String str : strs) {
				result.add(InetAddress.getByName(str.trim()));
			}
		} catch(UnknownHostException uhe) {
			return null;
		}
		return result;
	}
	
	public void setSeparatorString(String separator) {
		this.separator = separator;
	}
	
	@Override
	public String transformToStorageFormat(Set<InetAddress> value) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for(InetAddress addr: value) {
			if(!first) {
				sb.append(",");
			}
			sb.append(addr.toString());
			first = false;
		}
		return sb.toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean checkConstraintsOnValue(Object value) {
		if(value instanceof Set<?>) {
			for(Object obj: (Set<Object>)value) {
				if(!(obj instanceof InetAddress)) {
					return false;
				}
			}
			return true;
		}
		return false;
	}
}
