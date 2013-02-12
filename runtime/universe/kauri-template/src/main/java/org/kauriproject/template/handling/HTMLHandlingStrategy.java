package org.kauriproject.template.handling;

import java.io.IOException;

import javax.xml.parsers.SAXParser;

import org.kauriproject.util.xml.LocalDocumentBuilderFactory;
import org.kauriproject.xml.html.HtmlBodyContentsFilter;
import org.kauriproject.xml.html.NekoHtmlParser;
import org.w3c.dom.Document;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.sun.org.apache.xml.internal.utils.DOMBuilder;

public class HTMLHandlingStrategy implements HandlingStrategy {
    
    public void doInsert(HandlingInput input, final ContentHandler result, SAXParser parser) throws SAXException, IOException {
        //TODO check proper namespace-handling and correctly marking html to xhtml spaced elements
        // will be important for post-processing
        NekoHtmlParser neko = new NekoHtmlParser();
        neko.parse(input.getInputSource(), new HtmlBodyContentsFilter(result));
            
    }

    public Object parseToObject(HandlingInput input) throws SAXException, IOException {

            final Document doc = LocalDocumentBuilderFactory.newDocument();
            final DOMBuilder result = new DOMBuilder(doc);
            
            NekoHtmlParser neko = new NekoHtmlParser();
            neko.parse(input.getInputSource(), result);

            return doc;
    }
}