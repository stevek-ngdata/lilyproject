package org.lilyproject.tools.import_.json;

import java.util.Map;

public class UnmodifiableNamespaces implements Namespaces {
    private Namespaces delegate;

    public UnmodifiableNamespaces(Namespaces namespaces) {
        this.delegate = namespaces;
    }

    @Override
    public String getOrMakePrefix(String namespace) {
        throw new RuntimeException("This namespace context is not modifiable.");
    }

    @Override
    public void addMapping(String prefix, String namespace) {
        throw new RuntimeException("This namespace context is not modifiable.");
    }

    @Override
    public String getNamespace(String prefix) {
        return delegate.getNamespace(prefix);
    }

    @Override
    public String getPrefix(String namespace) {
        return delegate.getPrefix(namespace);
    }

    @Override
    public Map<String, String> getNsToPrefixMapping() {
        return delegate.getNsToPrefixMapping();
    }
}
