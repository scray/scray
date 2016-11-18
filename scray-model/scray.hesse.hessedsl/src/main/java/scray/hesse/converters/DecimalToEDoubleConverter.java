package scray.hesse.converters;

import org.eclipse.xtext.conversion.IValueConverter;
import org.eclipse.xtext.conversion.ValueConverterException;
import org.eclipse.xtext.nodemodel.INode;
import org.eclipse.xtext.util.Strings;

public class DecimalToEDoubleConverter implements IValueConverter<Double> {

	@Override
	public Double toValue(String string, INode node) throws ValueConverterException {
		if (Strings.isEmpty(string))
			throw new ValueConverterException("Couldn't convert empty string to a double value.", node, null);
		try {
			return Double.valueOf(string);
		} catch (NumberFormatException e) {
			throw new ValueConverterException("Couldn't convert '" + string + "' to a double value.", node, e);
		}
	}

	@Override
	public String toString(Double value) throws ValueConverterException {
		return value.toString();
	}
}
