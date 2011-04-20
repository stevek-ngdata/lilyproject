package org.lilyproject.tools.import_.json;

public class WriteOptions {
    public static final WriteOptions INSTANCE;
    static {
        INSTANCE = new WriteOptions();
        INSTANCE.makeImmutable();
    }

    private boolean immutable = false;

    /** For records, should schema information about the fields in the records be included? */
    private boolean includeSchema = false;

    public boolean getIncludeSchema() {
        return includeSchema;
    }

    public void setIncludeSchema(boolean includeSchema) {
        if (immutable)
            throw new RuntimeException("This WriteOptions instance is immutable.");

        this.includeSchema = includeSchema;
    }

    public void makeImmutable() {
        immutable = true;
    }
}
