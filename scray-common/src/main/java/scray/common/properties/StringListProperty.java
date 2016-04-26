package scray.common.properties;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Represents a property list of strings. Requires quoting of strings either with " or '.
 * Does not account for quotes in strings themselves.
 * @author andreas
 *
 */
public class StringListProperty extends Property<String, List<String>> {

	private String separator = ",";
	private String[] quotingChars = new String[] { "\\\"", "'" };
	
	private String name = null;
	private List<String> defaultValue = null;

	private void addQuotingRegex(StringBuilder sb) {
		sb.append("[");
		for(String quote: quotingChars) {
			sb.append(quote);
		}
		sb.append("]");		
	}
	
	private String getSeparatorRegex() {
		StringBuilder sb = new StringBuilder();
		addQuotingRegex(sb);
		sb.append("\\s*");
		sb.append(separator);
		sb.append("\\s*");
		addQuotingRegex(sb);
		return sb.toString();
	}
	
	public StringListProperty(String name) {
		this.name = name;
		super.addConstraint(new PropertyConstraint<String>() {
			public boolean checkConstraint(String value) {
				if(!(value instanceof String)) {
					return false;
				} else {
					String[] strs = value.split(getSeparatorRegex());
					return strs.length > 0;
				}
			}
		});
	}

	public StringListProperty(String name, List<String> defaultValue) {
		this(name);
		this.defaultValue = defaultValue;
	}
	
	@Override
	public boolean hasDefault() {
		return defaultValue != null;
	}

	@Override
	public List<String> getDefault() {
		return defaultValue;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public List<String> transformToResult(String value) {
		List<String> result = new ArrayList<String>();
		String[] strs = chop(value.trim()).split(getSeparatorRegex());
		for(String str : strs) {
			result.add(str);
		}
		return result;
	}
	
	private String chop(String str) {
		String tmp = str;
		for(String quote: quotingChars) {
			String q = quote;
			if(q.startsWith("\\")) {
				q = quote.substring(1);
			}
			if(str.startsWith(q)) {
				tmp = str.substring(q.length());
				break;
			}
		}
		for(String quote: quotingChars) {
			String q = quote;
			if(q.startsWith("\\")) {
				q = quote.substring(1);
			}
			if(tmp.endsWith(q)) {
				tmp = tmp.substring(0, tmp.length() - q.length());
				break;
			}
		}
		return tmp;
	}
	
	@Override
	public String transformToStorageFormat(List<String> value) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for(String str: value) {
			if(!first) {
				sb.append(", ");
			}
			sb.append(quotingChars[1]);
			sb.append(str);
			sb.append(quotingChars[1]);
			first = false;
		}
		return sb.toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean checkConstraintsOnValue(Object value) {
		if(value instanceof List<?>) {
			for(Object obj: (List<Object>)value) {
				if(!(obj instanceof String)) {
					return false;
				} 
			}
			return true;
		}
		return false;
	}
}
