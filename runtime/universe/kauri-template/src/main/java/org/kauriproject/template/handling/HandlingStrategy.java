package org.kauriproject.template.handling;

import java.io.IOException;

import javax.xml.parsers.SAXParser;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public interface HandlingStrategy {
    void doInsert(HandlingInput input, ContentHandler result, SAXParser parser) throws IOException, SAXException;
    Object parseToObject(HandlingInput input) throws IOException, SAXException;

}