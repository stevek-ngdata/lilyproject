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

import java.util.Map;

public class UnmodifiableNamespaces implements Namespaces {
    private Namespaces delegate;

    public UnmodifiableNamespaces(Namespaces namespaces) {
        this.delegate = namespaces;
    }

    @Override
    public boolean usePrefixes() {
        return delegate.usePrefixes();
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
