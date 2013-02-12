package org.kauriproject.template.handling;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;

import javax.xml.parsers.SAXParser;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

class TextHandlingStrategy implements HandlingStrategy {
    public void doInsert(HandlingInput input, final ContentHandler result, SAXParser parser) throws IOException, SAXException {
        
        //TODO check encoding used to read the text! -- possibly allow encoding to be specified or read from inputstream?
        
        Reader reader = input.getReader();
        char[] buf = new char[1024];
        int read = -1;
        while ( (read = reader.read(buf)) > 0) {
            result.characters(buf, 0, read);
        }
    }

    public Object parseToObject(HandlingInput input) throws SAXException, IOException {
        StringWriter writer = new StringWriter();
        
        //TODO reconsider this: sources can write to streams directly maybe some input.writeTo()
        
        Reader reader = input.getReader();
        char[] buf = new char[1024];
        int read = -1;
        while ( (read = reader.read(buf)) > 0) {
            writer.write(buf, 0, read);
        }
        return writer.toString();
    }
}