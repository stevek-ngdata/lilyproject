package org.lilycms.hbaseindex;

import java.util.HashMap;
import java.util.Map;

/**
 * An entry to add to or remove from an Index.
 *
 * <p>This object can be simply instantiated yourself and passed to
 * {@link Index#addEntry} or {@link Index#removeEntry}.
 */
public class IndexEntry {
    private Map<String, Object> values = new HashMap<String, Object>();

    public void addField(String name, Object value) {
        values.put(name, value);
    }

    public Object getValue(String name) {
        return values.get(name);
    }

    protected Map<String, Object> getValues() {
        return values;
    }
}
