package org.kauriproject.xml.html;

import java.io.IOException;

import org.cyberneko.html.parsers.SAXParser;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class NekoHtmlParser {
    public void parse(InputSource html, ContentHandler target) throws IOException, SAXException {
        if (html == null)
            throw new NullPointerException("html string argument is required.");

        SAXParser parser = new SAXParser();
        
        //for documentation on features and properties see http://nekohtml.sourceforge.net/settings.html
        
        parser.setFeature("http://xml.org/sax/features/namespaces", true);
        parser.setFeature("http://cyberneko.org/html/features/override-namespaces", false);
        parser.setFeature("http://cyberneko.org/html/features/insert-namespaces", false);
        parser.setFeature("http://cyberneko.org/html/features/scanner/ignore-specified-charset", true);
        parser.setProperty("http://cyberneko.org/html/properties/default-encoding", "UTF-8");
        parser.setProperty("http://cyberneko.org/html/properties/names/elems", "lower");
        parser.setProperty("http://cyberneko.org/html/properties/names/attrs", "lower");

        parser.setFeature("http://cyberneko.org/html/features/balance-tags/document-fragment", false);

        parser.setContentHandler(target);
        parser.parse(html);
    }
}
