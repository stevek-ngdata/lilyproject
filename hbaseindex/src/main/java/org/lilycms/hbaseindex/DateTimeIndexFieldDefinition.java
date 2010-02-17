package org.lilycms.hbaseindex;

import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.node.ObjectNode;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.lilycms.util.ArgumentValidator;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>TODO this class could be improved to handle negative years.
 */
public class DateTimeIndexFieldDefinition  extends IndexFieldDefinition {
    public enum Precision {DATETIME, DATETIME_NOMILLIS, DATE, TIME, TIME_NOMILLIS}
    private Precision precision = Precision.DATETIME_NOMILLIS;

    private static Map<Precision, Integer> LENGTHS;
    static {
        LENGTHS = new HashMap<Precision, Integer>();
        LENGTHS.put(Precision.DATETIME, "yyyyMMddTHHmmss.SSSZ".length());
        LENGTHS.put(Precision.DATETIME_NOMILLIS, "yyyyMMddTHHmmssZ".length());
        LENGTHS.put(Precision.DATE, "yyyyMMdd".length());
        LENGTHS.put(Precision.TIME, "HHmmss.SSSZ".length());
        LENGTHS.put(Precision.TIME_NOMILLIS, "HHmmssZ".length());
    }

    private static Map<Precision, DateTimeFormatter> FORMATTERS;
    static {
        FORMATTERS = new HashMap<Precision, DateTimeFormatter>();
        FORMATTERS.put(Precision.DATETIME, ISODateTimeFormat.basicDateTime().withZone(DateTimeZone.UTC));
        FORMATTERS.put(Precision.DATETIME_NOMILLIS, ISODateTimeFormat.basicDateTimeNoMillis().withZone(DateTimeZone.UTC));
        FORMATTERS.put(Precision.DATE, ISODateTimeFormat.basicDate().withZone(DateTimeZone.UTC));
        FORMATTERS.put(Precision.TIME, ISODateTimeFormat.basicTime().withZone(DateTimeZone.UTC));
        FORMATTERS.put(Precision.TIME_NOMILLIS, ISODateTimeFormat.basicTimeNoMillis().withZone(DateTimeZone.UTC));
    }
    
    public DateTimeIndexFieldDefinition(String name) {
        super(name, IndexValueType.DATETIME);
    }

    public DateTimeIndexFieldDefinition(String name, ObjectNode jsonObject) {
        this(name);

        if (jsonObject.get("precision") != null)
            this.precision = Precision.valueOf(jsonObject.get("precision").getTextValue());
    }

    public Precision getPrecision() {
        return precision;
    }

    public void setPrecision(Precision precision) {
        ArgumentValidator.notNull(precision, "precision");
        this.precision = precision;
    }

    @Override
    public int getByteLength() {
        Integer length = LENGTHS.get(precision);
        if (length == null) {
            throw new RuntimeException("Missing length for precision " + precision);
        }
        return length;
    }

    @Override
    public int toBytes(byte[] bytes, int offset, Object value) {
        return toBytes(bytes, offset, value, true);
    }

    @Override
    public int toBytes(byte[] bytes, int offset, Object value, boolean fillFieldLength) {
        DateTimeFormatter format = FORMATTERS.get(precision);
        String string = format.print(((Date)value).getTime());

        byte[] datetimeBytes = Bytes.toBytes(string);

        if (datetimeBytes.length != getByteLength()) {
            throw new RuntimeException("Unexpected situation: byte-formatted datetime is " + datetimeBytes.length
                    + " bytes long, but expected " + getByteLength() + ", date = " + string);
        }

        System.arraycopy(datetimeBytes, 0, bytes, 0, datetimeBytes.length);

        return offset + datetimeBytes.length;
    }

    @Override
    public ObjectNode toJson() {
        ObjectNode object = super.toJson();
        object.put("precision", precision.toString());
        return object;
    }
}

