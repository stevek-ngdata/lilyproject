package org.lilycms.hbaseindex;

import java.util.HashMap;
import java.util.Map;

public class IndexEntry {
    private Map<String, Object> values = new HashMap<String, Object>();

    public void addField(String name, Object value) {
        values.put(name, value);
    }

    public Object getValue(String name) {
        return values.get(name);
    }
}
