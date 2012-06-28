package org.lilyproject.util.repo;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.ValueType;

import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Defines conversion from/to string for a subset of the Lily {@link ValueType}s.
 *
 * <p>Complex types such as lists and records are not supported.</p>
 */
public class FieldValueStringConverter {
    public static String toString(Object value, ValueType valueType) {
        if (value == null) {
            throw new NullPointerException();
        }

        if (!valueType.getType().isAssignableFrom(value.getClass())) {
            throw new IllegalArgumentException("Value is not of the type indicated by the value type. Value type" +
                    " class: " + valueType.getType().getName() + ", value class: " + value.getClass().getName());
        }

        StringConverter converter = CONVERTERS.get(valueType.getBaseName());
        if (converter == null) {
            throw new IllegalArgumentException("Value type not supported: " + valueType.getBaseName());
        }

        return converter.toString(value);
    }

    /**
     *
     * @throws IllegalArgumentException if the value cannot be parsed or if the value type
     *                                  is not supported
     */
    public static Object fromString(String value, ValueType valueType, IdGenerator idGenerator) {
        StringConverter converter = CONVERTERS.get(valueType.getBaseName());
        if (converter == null) {
            throw new IllegalArgumentException("Value type not supported: " + valueType.getBaseName());
        }

        return converter.fromString(value, idGenerator);
    }

    private static Map<String, StringConverter> CONVERTERS;
    static {
        CONVERTERS = new HashMap<String, StringConverter>();
        CONVERTERS.put("STRING", new StringStringConverter());
        CONVERTERS.put("INTEGER", new IntegerStringConverter());
        CONVERTERS.put("LONG", new LongStringConverter());
        CONVERTERS.put("DOUBLE", new DoubleStringConverter());
        CONVERTERS.put("DECIMAL", new DecimalStringConverter());
        CONVERTERS.put("URI", new UriStringConverter());
        CONVERTERS.put("BOOLEAN", new BooleanStringConverter());
        CONVERTERS.put("LINK", new LinkStringConverter());
        CONVERTERS.put("DATE", new DateStringConverter());
        CONVERTERS.put("DATETIME", new DateTimeStringConverter());
    }

    public interface StringConverter {
        String toString(Object value);

        Object fromString(String value, IdGenerator idGenerator);
    }

    public static class StringStringConverter implements StringConverter {
        @Override
        public String toString(Object value) {
            return (String)value;
        }

        @Override
        public Object fromString(String value, IdGenerator idGenerator) {
            return value;
        }
    }

    public static class IntegerStringConverter implements StringConverter {
        @Override
        public String toString(Object value) {
            return value.toString();
        }

        @Override
        public Object fromString(String value, IdGenerator idGenerator) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid integer value: " + value, e);
            }
        }
    }

    public static class LongStringConverter implements StringConverter {
        @Override
        public String toString(Object value) {
            return value.toString();
        }

        @Override
        public Object fromString(String value, IdGenerator idGenerator) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid long value: " + value, e);
            }
        }
    }

    public static class DoubleStringConverter implements StringConverter {
        @Override
        public String toString(Object value) {
            return value.toString();
        }

        @Override
        public Object fromString(String value, IdGenerator idGenerator) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid double value: " + value, e);
            }
        }
    }

    public static class DecimalStringConverter implements StringConverter {
        @Override
        public String toString(Object value) {
            return value.toString();
        }

        @Override
        public Object fromString(String value, IdGenerator idGenerator) {
            try {
                return new BigDecimal(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid decimal value: " + value, e);
            }
        }
    }

    public static class LinkStringConverter implements StringConverter {
        @Override
        public String toString(Object value) {
            return value.toString();
        }

        @Override
        public Object fromString(String value, IdGenerator idGenerator) {
            return Link.fromString(value, idGenerator);
        }
    }

    public static class UriStringConverter implements StringConverter {
        @Override
        public String toString(Object value) {
            return value.toString();
        }

        @Override
        public Object fromString(String value, IdGenerator idGenerator) {
            try {
                return new URI(value);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid URI value: " + value, e);
            }
        }
    }

    public static class BooleanStringConverter implements StringConverter {
        @Override
        public String toString(Object value) {
            return value.toString();
        }

        @Override
        public Object fromString(String value, IdGenerator idGenerator) {
            if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("t")
                    || value.equalsIgnoreCase("yes") || value.equalsIgnoreCase("y")) {
                return Boolean.TRUE;
            } else {
                return Boolean.FALSE;
            }
        }
    }

    public static class DateStringConverter implements StringConverter {
        @Override
        public String toString(Object value) {
            return value.toString();
        }

        @Override
        public Object fromString(String value, IdGenerator idGenerator) {
            return new LocalDate(value);
        }
    }

    public static class DateTimeStringConverter implements StringConverter {
        @Override
        public String toString(Object value) {
            return value.toString();
        }

        @Override
        public Object fromString(String value, IdGenerator idGenerator) {
            return new DateTime(value, DateTimeZone.UTC);
        }
    }
}
