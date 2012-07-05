/*
 * Copyright 2010 Outerthought bvba
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
package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.util.repo.SystemFields;
import org.w3c.dom.Element;

public class ConfUtil {

    public static QName parseQName(String qname, Element contextEl) throws IndexerConfException {
        return parseQName(qname, contextEl, false);
    }

    public static QName parseQName(String qname, Element contextEl, boolean prefixResolvingOptional)
            throws IndexerConfException {
        // The qualified name can either be specified in Lily-style ("{namespace}name") or
        // in XML-style (prefix:name). In Lily-style, if the "namespace" matches a defined
        // prefix, it is substituted.

        if (qname.startsWith("{")) {
            QName name = QName.fromString(qname);
            String ns = contextEl.lookupNamespaceURI(name.getNamespace());
            if (ns != null) {
                return new QName(ns, name.getName());
            } else {
                return name;
            }
        }

        int colonPos = qname.indexOf(":");
        if (colonPos == -1) {
            throw new IndexerConfException(
                    "Field name is not a qualified name, it should include a namespace prefix: " + qname);
        }

        String prefix = qname.substring(0, colonPos);
        String localName = qname.substring(colonPos + 1);

        String uri = contextEl.lookupNamespaceURI(prefix);
        if (uri == null && !prefixResolvingOptional) {
            throw new IndexerConfException("Prefix does not resolve to a namespace: " + qname);
        }
        if (uri == null) {
            uri = prefix;
        }

        return new QName(uri, localName);
    }

    public static FieldType getFieldType(String qname, Element contextEl, SystemFields systemFields, TypeManager typeManager) throws IndexerConfException, InterruptedException,
            RepositoryException {
        QName parsedQName = parseQName(qname, contextEl);
        return getFieldType(parsedQName, systemFields, typeManager);
    }

    public static FieldType getFieldType(QName qname, SystemFields systemFields, TypeManager typeManager) throws IndexerConfException, InterruptedException,
            RepositoryException {

        if (systemFields.isSystemField(qname)) {
            return systemFields.get(qname);
        }

        return typeManager.getFieldTypeByName(qname);
    }

}
