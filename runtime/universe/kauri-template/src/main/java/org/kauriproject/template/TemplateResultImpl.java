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

import org.kauriproject.xml.sax.XmlConsumer;
import org.kauriproject.xml.sax.SaxBuffer;
import org.kauriproject.xml.sax.NopXmlConsumer;
import org.kauriproject.xml.sax.XmlFilter;
import org.xml.sax.SAXException;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.AttributesImpl;

import java.util.List;
import java.util.ArrayList;

public class TemplateResultImpl extends NopXmlConsumer implements TemplateResult {
    private XmlConsumer next;
    private XmlConsumer finalConsumer;
    private List<XmlFilter> filters = new ArrayList<XmlFilter>();
    
    // The buffers are to be able to add attribute events to the last startElement
    // Until there are only character events after the last startElement, we also buffer those
    // TODO: normally that should only be strippeable whitespace, so we don't really need this buffer?
    //        this concept was taken from CTemplate, where no whitespace is ever stripped
    private SaxBuffer.StartElement bufferedStartElement;
    private SaxBuffer bufferedContent = new SaxBuffer();

    public TemplateResultImpl(XmlConsumer next) {
        this.finalConsumer = next;
        setupFilters();
    }

    public void flush() throws SAXException {
        if (bufferedStartElement != null) {
            bufferedStartElement.send(next);
            bufferedStartElement = null;
        }
        if (!bufferedContent.isEmpty()) {
            bufferedContent.toSAX(next);
            bufferedContent.recycle();
        }
    }

    public void addFilter(XmlFilter filter) {
        filters.add(filter);
        setupFilters();
    }

    public void popFilter() {
        filters.remove(filters.size() - 1);
        setupFilters();
    }

    public void setupFilters() {
        XmlConsumer nextConsumer = finalConsumer;
        for (XmlFilter filter : filters) {
            filter.setConsumer(nextConsumer);
            nextConsumer = filter;
        }
        this.next = nextConsumer;
    }

    public void addAttribute(String uri, String localName, String qName, String type, String value) throws SAXException {
        if (bufferedStartElement != null) {
            ((AttributesImpl)bufferedStartElement.attrs).addAttribute(uri, localName, qName, type, value);
        } else {
            throw new SAXException("Got an attribute event but there was no start element event available to add it to.");
        }
    }

    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        flush();
        SaxBuffer.StartElement startEl = new SaxBuffer.StartElement(uri, localName, qName, attributes);
        this.bufferedStartElement = startEl;
    }

    public void endElement(String uri, String localName, String qName) throws SAXException {
        flush();
        next.endElement(uri, localName, qName);
    }

    public void startDocument() throws SAXException {
        flush();
        next.startDocument();
    }

    public void endDocument() throws SAXException {
        flush();
        next.endDocument();
    }

    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        flush();
        next.startPrefixMapping(prefix, uri);
    }

    public void endPrefixMapping(String prefix) throws SAXException {
        flush();
        next.endPrefixMapping(prefix);
    }

    public void characters(char ch[], int start, int length) throws SAXException {
        if (bufferedStartElement != null) {
            bufferedContent.characters(ch, start, length);
            // TODO maybe flush if there are more than 5 events waiting or so
        } else {
            flush();
            next.characters(ch, start, length);
        }
    }

    public void ignorableWhitespace(char ch[], int start, int length) throws SAXException {
        if (bufferedStartElement != null) {
            bufferedContent.ignorableWhitespace(ch, start, length);
        } else {
            flush();
            next.ignorableWhitespace(ch, start, length);
        }
    }

    public void processingInstruction(String target, String data) throws SAXException {
        flush();
        next.processingInstruction(target, data);
    }

    public void comment(char ch[], int start, int length) throws SAXException {
        flush();
        next.comment(ch, start, length);
    }
}
