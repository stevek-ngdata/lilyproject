package org.lilyproject.tools.import_.json;

import java.util.HashMap;
import java.util.Map;

/**
 * Combines globally and locally defined namespaces into one namespaces context, i.e. provides
 * for one level of namespace inheritance. (Just one level because that is enough, could be easily
 * extended to supporting any level if necessary).
 *
 * <p>This should typically be used in reading-situations, not in writing-situations.</p>
 */
public class InheritingNamespaces implements Namespaces {
    private Namespaces parent;
    private Namespaces child;

    public InheritingNamespaces(Namespaces parent, Namespaces child) {
        this.parent = parent;
        this.child = child;
    }

    @Override
    public boolean usePrefixes() {
        return child.usePrefixes();
    }

    @Override
    public String getOrMakePrefix(String namespace) {
        throw new RuntimeException("This namespace context is not modifiable.");
        /*
        if (child.getPrefix(namespace) != null) {
            return child.getPrefix(namespace);
        } else if (parent.getPrefix(namespace) != null) {
            return parent.getPrefix(namespace);
        } else {
            return child.getOrMakePrefix(namespace);
        }
        */
    }

    @Override
    public void addMapping(String prefix, String namespace) {
        throw new RuntimeException("This namespace context is not modifiable.");
        // child.addMapping(prefix, namespace);
    }

    @Override
    public String getNamespace(String prefix) {
        if (child.getNamespace(prefix) != null) {
            return child.getNamespace(prefix);
        } else {
            return parent.getNamespace(prefix);
        }
    }

    @Override
    public String getPrefix(String namespace) {
        if (child.getPrefix(namespace) != null) {
            return child.getPrefix(namespace);
        } else {
            return parent.getPrefix(namespace);
        }
    }

    @Override
    public Map<String, String> getNsToPrefixMapping() {
        Map<String, String> result = new HashMap<String, String>(parent.getNsToPrefixMapping());
        result.putAll(child.getNsToPrefixMapping());
        return result;
    }
}
