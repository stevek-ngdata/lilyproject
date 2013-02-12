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

import javax.xml.transform.sax.TransformerHandler;

import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

/**
 * Adapt TransformerHandler to our XmlConsumer interface.
 */
public class TransformerHandlerAdapter implements XmlConsumer {

    private TransformerHandler handler;

    public TransformerHandlerAdapter(TransformerHandler handler) {
        this.handler = handler;
    }

    public void characters(char[] ch, int start, int length) throws SAXException {
        handler.characters(ch, start, length);
    }

    public void endDocument() throws SAXException {
        handler.endDocument();
    }

    public void endElement(String uri, String localName, String name) throws SAXException {
        handler.endElement(uri, localName, name);
    }

    public void endPrefixMapping(String prefix) throws SAXException {
        handler.endPrefixMapping(prefix);
    }

    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        handler.ignorableWhitespace(ch, start, length);
    }

    public void processingInstruction(String target, String data) throws SAXException {
        handler.processingInstruction(target, data);
    }

    public void setDocumentLocator(Locator locator) {
        handler.setDocumentLocator(locator);
    }

    public void skippedEntity(String name) throws SAXException {
        handler.skippedEntity(name);
    }

    public void startDocument() throws SAXException {
        handler.startDocument();
    }

    public void startElement(String uri, String localName, String name, Attributes atts) throws SAXException {
        handler.startElement(uri, localName, name, atts);
    }

    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        handler.startPrefixMapping(prefix, uri);
    }

    public void comment(char[] ch, int start, int length) throws SAXException {
        handler.comment(ch, start, length);
    }

    public void endCDATA() throws SAXException {
        handler.endCDATA();
    }

    public void endDTD() throws SAXException {
        handler.endDTD();
    }

    public void endEntity(String name) throws SAXException {
        handler.endEntity(name);
    }

    public void startCDATA() throws SAXException {
        handler.startCDATA();
    }

    public void startDTD(String name, String publicId, String systemId) throws SAXException {
        handler.startDTD(name, publicId, systemId);
    }

    public void startEntity(String name) throws SAXException {
        handler.startEntity(name);
    }

}
