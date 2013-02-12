package org.kauriproject.template.handling;

import java.io.IOException;

import javax.xml.parsers.SAXParser;

import org.kauriproject.util.io.IOUtils;
import org.kauriproject.util.xml.LocalDocumentBuilderFactory;
import org.kauriproject.xml.sax.EmbedXmlFilter;
import org.kauriproject.xml.sax.XmlConsumer;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

class XMLHandlingStrategy implements HandlingStrategy {
    public void doInsert(HandlingInput input, final ContentHandler result, final SAXParser parser) throws IOException, SAXException {
        // default to XML: parse with saxparser
        XmlConsumer consumer = new EmbedXmlFilter(result);
        XMLReader xmlReader = parser.getXMLReader();
        xmlReader.setContentHandler(consumer);
        xmlReader.setProperty("http://xml.org/sax/properties/lexical-handler", consumer);
        xmlReader.parse(input.getInputSource());
    }

    public Object parseToObject(HandlingInput input) throws SAXException, IOException {
        InputSource is = null;
        try {
            is = input.getInputSource();
            return LocalDocumentBuilderFactory.getBuilder().parse(is);
        } finally {
            IOUtils.closeQuietly(input);
        }
    }
}