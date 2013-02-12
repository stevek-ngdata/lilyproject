package org.kauriproject.xml.html;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

public class HtmlBodyContentsFilter implements ContentHandler {
    private final ContentHandler consumer;
    private int depthInBody = 0;
    
    public HtmlBodyContentsFilter(final ContentHandler consumer) {
        this.consumer = consumer;
    }

    private boolean inBody() {
        return this.depthInBody > 0;
    }

    public void endDocument() throws SAXException {
        // dropped on purpose
    }

    public void startDocument() throws SAXException {
        // dropped on purpose
    }

    public void processingInstruction(String target, String data) throws SAXException {
        // dropped on purpose
    }

    public void characters(char ch[], int start, int length) throws SAXException {
        if (inBody())
            consumer.characters(ch, start, length);
    }

    public void ignorableWhitespace(char ch[], int start, int length) throws SAXException {
        if (inBody())
            consumer.ignorableWhitespace(ch, start, length);
    }

    public void endPrefixMapping(String prefix) throws SAXException {
        consumer.endPrefixMapping(prefix);
    }

    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        consumer.startPrefixMapping(prefix, uri);
    }

    public void skippedEntity(String name) throws SAXException {
        if (inBody())
            consumer.skippedEntity(name);
    }

    public void setDocumentLocator(Locator locator) {
        consumer.setDocumentLocator(locator);
    }

    public void endElement(String namespaceURI, String localName, String qName) throws SAXException {
        if (this.depthInBody > 1) { // body itself can't be ended! 
            consumer.endElement(namespaceURI, localName, localName);
            this.depthInBody--;
        }
    }

    public void startElement(String namespaceURI, String localName, String qName, Attributes atts) throws SAXException {
        if (inBody()) {
            consumer.startElement(namespaceURI, localName, localName, atts);
            this.depthInBody++;
        } else {
            if (localName.equals("body"))
                this.depthInBody = 1;
        }
    }
}