package org.lilycms.hbaseindex;

public enum IndexValueType {
    STRING(java.lang.String.class),
    INTEGER(java.lang.Integer.class),
    FLOAT(java.lang.Float.class),
    DATETIME(java.util.Date.class),
    DECIMAL(java.math.BigDecimal.class);

    private Class clazz;

    private IndexValueType(Class clazz) {
        this.clazz = clazz;
    }

    public boolean supportsType(Class clazz) {
        return this.clazz.isAssignableFrom(clazz);
    }

    public Class getType() {
        return clazz;
    }
}
