package org.kauriproject.template.handling;

import java.io.IOException;

import javax.xml.parsers.SAXParser;

import org.kauriproject.util.xml.XmlMediaTypeHelper;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public enum Handling implements HandlingStrategy{
    XML("xml", new XMLHandlingStrategy()), 
    JSON("json", new JSONHandlingStrategy()),
    TXT("txt", new TextHandlingStrategy()),
    HTML("html", new HTMLHandlingStrategy());

    private final String mode;
    private final HandlingStrategy strategy;
    
    private Handling(final String mode, final HandlingStrategy strategy){
        this.mode = mode;
        this.strategy = strategy;
    }
    
    public void doInsert(HandlingInput input, ContentHandler result, SAXParser parser) throws IOException, SAXException{
        this.strategy.doInsert(input, result, parser);
    }

    public Object parseToObject(HandlingInput input) throws IOException, SAXException {
        return this.strategy.parseToObject(input);
    }
    
    public static Handling fromMode(final String lookupMode) {
        for (Handling candidate : Handling.values()) {
            if (candidate.mode.equals(lookupMode))
                return candidate;
        }
        return null; // none found
    }

    public static Handling fromMediaType(String mime) {
        if (XmlMediaTypeHelper.isXmlMediaType(mime)) {
            return XML;
        } else if (mime.equals("application/json")) {
            return JSON;
        } else if (mime.startsWith("text/html")) {
            return HTML;
        } else if (mime.startsWith("text")) {
            return TXT;
        } else {
            return null;
        }
    }

}