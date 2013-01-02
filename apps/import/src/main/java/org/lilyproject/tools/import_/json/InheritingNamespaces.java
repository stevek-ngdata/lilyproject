/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.tools.import_.json;

import java.util.HashMap;
import java.util.Map;

/**
 * Combines globally and locally defined namespaces into one namespaces context, i.e. provides
 * for one level of namespace inheritance. (Just one level because that is enough, could be easily
 * extended to supporting any level if necessary).
 * <p/>
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
