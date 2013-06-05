package org.lilyproject.tools.import_.json;

public class IgnoreAndDeleteEmptyFieldsRecordReader extends IgnoreEmptyFieldsRecordReader {
    public static final IgnoreAndDeleteEmptyFieldsRecordReader INSTANCE = new IgnoreAndDeleteEmptyFieldsRecordReader();

    @Override
    protected boolean deleteNullFields() {
        return true;
    }
}
