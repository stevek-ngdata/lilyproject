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
package org.kauriproject.xml.sax;

import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.ext.LexicalHandler;

/**
 * A class which receives SAX events and produces SAX events towards a consumer.
 */
public class XmlFilter extends AbstractXmlProducer implements XmlConsumer {

    public XmlFilter() {
        super();
    }

    public XmlFilter(ContentHandler contentHandler) {
        super(contentHandler);
    }

    public XmlFilter(ContentHandler contentHandler, LexicalHandler lexicalHandler) {
        super(contentHandler, lexicalHandler);
    }

    public XmlFilter(XmlConsumer consumer) {
        super(consumer);
    }

    public void setDocumentLocator(Locator locator) {
        if (this.contentHandler != null)
            contentHandler.setDocumentLocator(locator);
    }

    public void startDocument() throws SAXException {
        if (this.contentHandler != null)
            contentHandler.startDocument();
    }

    public void endDocument() throws SAXException {
        if (this.contentHandler != null)
            contentHandler.endDocument();
    }

    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        if (this.contentHandler != null)
            contentHandler.startPrefixMapping(prefix, uri);
    }

    public void endPrefixMapping(String prefix) throws SAXException {
        if (this.contentHandler != null)
            contentHandler.endPrefixMapping(prefix);
    }

    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        if (this.contentHandler != null)
            contentHandler.startElement(uri, localName, qName, atts);
    }

    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (this.contentHandler != null)
            contentHandler.endElement(uri, localName, qName);
    }

    public void characters(char[] ch, int start, int length) throws SAXException {
        if (this.contentHandler != null)
            contentHandler.characters(ch, start, length);
    }

    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        if (this.contentHandler != null)
            contentHandler.ignorableWhitespace(ch, start, length);
    }

    public void processingInstruction(String target, String data) throws SAXException {
        if (this.contentHandler != null)
            contentHandler.processingInstruction(target, data);
    }

    public void skippedEntity(String name) throws SAXException {
        if (this.contentHandler != null)
            contentHandler.skippedEntity(name);
    }

    public void startDTD(String name, String publicId, String systemId) throws SAXException {
        if (this.lexicalHandler != null)
            lexicalHandler.startDTD(name, publicId, systemId);
    }

    public void endDTD() throws SAXException {
        if (this.lexicalHandler != null)
            lexicalHandler.endDTD();
    }

    public void startEntity(String name) throws SAXException {
        if (this.lexicalHandler != null)
            lexicalHandler.startEntity(name);
    }

    public void endEntity(String name) throws SAXException {
        if (this.lexicalHandler != null)
            lexicalHandler.endEntity(name);
    }

    public void startCDATA() throws SAXException {
        if (this.lexicalHandler != null)
            lexicalHandler.startCDATA();
    }

    public void endCDATA() throws SAXException {
        if (this.lexicalHandler != null)
            lexicalHandler.endCDATA();
    }

    public void comment(char[] ch, int start, int length) throws SAXException {
        if (this.lexicalHandler != null)
            lexicalHandler.comment(ch, start, length);
    }
}
