package org.lilyproject.util.repo.test;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.junit.Test;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.util.repo.FieldValueStringConverter;

import java.math.BigDecimal;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FieldValueStringConverterTest {
    @Test
    public void testFromString() throws Exception {
        IdGenerator idGenerator = new IdGeneratorImpl();

        ValueType valueType = mock(ValueType.class);

        when(valueType.getBaseName()).thenReturn("STRING");
        assertEquals("foo", FieldValueStringConverter.fromString("foo", valueType, idGenerator));

        when(valueType.getBaseName()).thenReturn("INTEGER");
        assertEquals(new Integer(123), FieldValueStringConverter.fromString("123", valueType, idGenerator));

        when(valueType.getBaseName()).thenReturn("LONG");
        assertEquals(new Long(12345), FieldValueStringConverter.fromString("12345", valueType, idGenerator));

        when(valueType.getBaseName()).thenReturn("DOUBLE");
        assertEquals(new Double(12345.6), FieldValueStringConverter.fromString("12345.6", valueType, idGenerator));

        when(valueType.getBaseName()).thenReturn("DECIMAL");
        assertEquals(new BigDecimal("12345.12345"), FieldValueStringConverter.fromString("12345.12345", valueType, idGenerator));

        when(valueType.getBaseName()).thenReturn("URI");
        assertEquals(new URI("http://www.ngdata.com/"),
                FieldValueStringConverter.fromString("http://www.ngdata.com/", valueType, idGenerator));

        when(valueType.getBaseName()).thenReturn("BOOLEAN");
        assertEquals(Boolean.TRUE, FieldValueStringConverter.fromString("true", valueType, idGenerator));
        assertEquals(Boolean.FALSE, FieldValueStringConverter.fromString("false", valueType, idGenerator));

        when(valueType.getBaseName()).thenReturn("LINK");
        assertEquals(new Link(idGenerator.newRecordId("foobar")),
                FieldValueStringConverter.fromString("USER.foobar", valueType, idGenerator));

        when(valueType.getBaseName()).thenReturn("DATE");
        assertEquals(new LocalDate(2012, 6, 28), FieldValueStringConverter.fromString("2012-06-28", valueType, idGenerator));

        when(valueType.getBaseName()).thenReturn("DATETIME");
        assertEquals(new DateTime(2012, 6, 28, 5, 36, 0, 0, DateTimeZone.UTC),
                FieldValueStringConverter.fromString("2012-06-28T05:36Z", valueType, idGenerator));
    }
}
