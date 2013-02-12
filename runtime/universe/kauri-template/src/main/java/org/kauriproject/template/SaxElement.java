/*
 * Copyright 2008 Outerthought bvba and Schaubroeck nv
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
package org.kauriproject.template;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.AttributesImpl;

/**
 * Eenvoudige wrapper rond een saxelement
 * 
 * TODO: extends Step ? StartElement ipv SaxElement ?
 */
public class SaxElement {

    private String uri;
    private String localName;
    private String name;
    private AttributesImpl attributes;

    public SaxElement(String uri, String localName, String name, Attributes attributes) {
        super();
        this.uri = uri;
        this.localName = localName;
        this.name = name;
        this.attributes = new AttributesImpl(attributes);// argument is proxy
    }

    public String getUri() {
        return uri;
    }

    public String getLocalName() {
        return localName;
    }

    public String getName() {
        return name;
    }

    public AttributesImpl getAttributes() {
        return attributes;
    }

    public void setAttributes(AttributesImpl attributes) {
        this.attributes = attributes;
    }
}
