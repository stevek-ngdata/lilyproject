package org.lilycms.hbaseindex;

import org.apache.hadoop.hbase.util.Bytes;

public class StringIndexFieldDefinition extends IndexFieldDefinition {
    private int length = 100;

    public StringIndexFieldDefinition(String name) {
        super(name, IndexValueType.STRING);
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getByteLength() {
        return length;
    }

    public int toBytes(byte[] bytes, int offset, Object value) {
        String string = (String)value;
        byte[] stringAsBytes = Bytes.toBytes(string);


        if (stringAsBytes.length < length) {
            byte[] buffer = new byte[length];
            System.arraycopy(stringAsBytes, 0, buffer, 0, stringAsBytes.length);
            stringAsBytes = buffer;
        }

        // TODO: is it ok that we cut of an UTF-8 string at an arbitrary position?
        return Bytes.putBytes(bytes, offset, stringAsBytes, 0, length);
    }

}
