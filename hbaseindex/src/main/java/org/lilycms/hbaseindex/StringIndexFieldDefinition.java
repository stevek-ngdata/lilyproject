package org.lilycms.hbaseindex;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.util.ArgumentValidator;

import java.io.UnsupportedEncodingException;
import java.text.Collator;
import java.text.Normalizer;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class StringIndexFieldDefinition extends IndexFieldDefinition {
    public enum ByteEncodeMode { UTF8, ASCII_FOLDING, COLLATOR }

    private int length = 100;
    private Locale locale = Locale.US;
    private ByteEncodeMode byteEncodeMode = ByteEncodeMode.UTF8;
    private boolean caseSensitive = true;

    private static Map<ByteEncodeMode, StringEncoder> ENCODERS;
    static {
        ENCODERS = new HashMap<ByteEncodeMode, StringEncoder>();
        ENCODERS.put(ByteEncodeMode.UTF8, new Utf8StringEncoder());
        ENCODERS.put(ByteEncodeMode.ASCII_FOLDING, new AsciiFoldingStringEncoder());
        ENCODERS.put(ByteEncodeMode.COLLATOR, new CollatorStringEncoder());
    }

    public StringIndexFieldDefinition(String name) {
        super(name, IndexValueType.STRING);
    }

    /**
     * The number of bytes this field takes in a index entry. This is
     * not the same as the number of characters, the number of bytes
     * needed for one characters depends on the mode. 
     */
    public void setLength(int length) {
        this.length = length;
    }

    public Locale getLocale() {
        return locale;
    }

    /**
     * The Locale to use. This locale is used to fold the case when
     * case sensitivity is not desired, and is used in case the
     * {@link ByteEncodeMode#COLLATOR} mode is selected.
     */
    public void setLocale(Locale locale) {
        ArgumentValidator.notNull(locale, "locale");
        this.locale = locale;
    }

    public ByteEncodeMode getByteEncodeMode() {
        return byteEncodeMode;
    }

    /**
     * Sets the way the string should be converted to bytes. This will
     * determine how the items are ordered in the index (which is important
     * for range searches) and also whether searches are sensitive to accents
     * and the like.
     */
    public void setByteEncodeMode(ByteEncodeMode byteEncodeMode) {
        ArgumentValidator.notNull(byteEncodeMode, "byteEncodeMode");
        this.byteEncodeMode = byteEncodeMode;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    /**
     * Indicates if the string index should be case sensitive.
     */
    public void setCaseSensitive(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
    }

    public int getByteLength() {
        return length;
    }

    public int toBytes(byte[] bytes, int offset, Object value) {
        String string = (String)value;
        string = Normalizer.normalize(string, Normalizer.Form.NFC);

        if (!caseSensitive) {
            string = string.toLowerCase(locale);
        }

        byte[] stringAsBytes = ENCODERS.get(byteEncodeMode).toBytes(string, locale);

        if (stringAsBytes.length < length) {
            byte[] buffer = new byte[length];
            System.arraycopy(stringAsBytes, 0, buffer, 0, stringAsBytes.length);
            stringAsBytes = buffer;
        }

        return Bytes.putBytes(bytes, offset, stringAsBytes, 0, length);
    }

    private interface StringEncoder {
        byte[] toBytes(String string, Locale locale);
    }

    private static class Utf8StringEncoder implements StringEncoder {
        public byte[] toBytes(String string, Locale locale) {
            try {
                return string.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class AsciiFoldingStringEncoder implements StringEncoder {
        public byte[] toBytes(String string, Locale locale) {
            try {
                return ASCIIFoldingUtil.foldToASCII(string).getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class CollatorStringEncoder implements StringEncoder {
        public byte[] toBytes(String string, Locale locale) {
            Collator collator = Collator.getInstance(locale);
            return collator.getCollationKey(string).toByteArray();
        }
    }
}
