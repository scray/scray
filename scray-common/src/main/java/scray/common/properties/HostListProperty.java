package scray.common.properties;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class HostListProperty extends Property<String, List<InetAddress>> {

	private String separator = ",";
	
	private String name = null;
	private List<InetAddress> defaultValue = null;

	public HostListProperty(String name) {
		this.name = name;
		super.addConstraint(new PropertyConstraint<String>() {
			public boolean checkConstraint(String value) {
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

	public HostListProperty(String name, List<InetAddress> defaultValue) {
		this(name);
		this.defaultValue = defaultValue;
	}
	
	@Override
	public boolean hasDefault() {
		return defaultValue != null;
	}

	@Override
	public List<InetAddress> getDefault() {
		return defaultValue;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public List<InetAddress> transformToResult(String value) {
		List<InetAddress> result = new ArrayList<InetAddress>();
		try {
			String[] strs = value.split(separator);
			for(String str : strs) {
				result.add(InetAddress.getByName(str));
			}
		} catch(UnknownHostException uhe) {
			return null;
		}
		return result;
	}
	
	public void setSeparatorString(String separator) {
		this.separator = separator;
	}
}
