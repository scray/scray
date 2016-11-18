package scray.hesse.converters;

import org.eclipse.xtext.common.services.DefaultTerminalConverters;
import org.eclipse.xtext.conversion.IValueConverter;
import org.eclipse.xtext.conversion.ValueConverter;

public class HesseTerminalConverters extends DefaultTerminalConverters {

    @ValueConverter(rule = "ConstantDoubleColumnvalue")
    public IValueConverter<Double> getConstantDoubleColumnvalueConverter() {
      return new DecimalToEDoubleConverter();
    }

    @ValueConverter(rule = "ConstantLongValue")
    public IValueConverter<Long> getConstantLongValueConverter() {
      return new LongToELongConverter();
    }
}
