package org.lilyproject.runtime.conf.jxpath;

public class ConfAttr {
    private String name;
    private String value;

    public ConfAttr(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ConfAttr))
            return false;

        ConfAttr attr = (ConfAttr)obj;
        return attr.name.equals(name) && attr.value.equals(value);
    }

    @Override
    public int hashCode() {
        return (name + value).hashCode();
    }
}
