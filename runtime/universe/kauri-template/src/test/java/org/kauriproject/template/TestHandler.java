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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kauriproject.xml.sax.XmlConsumer;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.AttributesImpl;

/**
 * Handler that can be used to format a template result and write it to an outputstream. The result is a
 * simple xml representation of the template result.
 * 
 * NOTE: whitespace is ignored, this is OK for now because we test in the first place the working of the
 * template directives.
 */
public class TestHandler extends DefaultHandler implements XmlConsumer {

    private static Log log = LogFactory.getLog(TestHandler.class);

    /**
     * Indentation used to format the XML output.
     */
    private static final String INDENT = "  ";

    /**
     * Used to keep track of depth for formatting purposes.
     */
    private int currentDepth;

    /**
     * Track the count of charevents that should be handled together. If our template engine works properly,
     * this should never be bigger than 1.
     */
    private int chareventCount;

    /**
     * Buffer for characters.
     */
    private StringBuffer buffer = new StringBuffer();

    private BufferedWriter writer;

    public TestHandler() {
        this(System.out);
    }

    public TestHandler(OutputStream out) {
        this.writer = new BufferedWriter(new OutputStreamWriter(out));
        this.currentDepth = 0;
        this.chareventCount = 0;
    }

    /**
     * Formats the XML by adding indentation.
     */
    private String indentLine(final String line) {
        StringBuffer sb = new StringBuffer(line);
        for (int i = 1; i < currentDepth; i++) {
            sb.insert(0, INDENT);
        }
        return sb.toString();
    }

    private void writeLine(final String line) {
        try {
            writer.write(line);
            writer.newLine();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private void writeBuffer() {
        if (chareventCount > 0) {
            String string = buffer.toString().trim();
            if (string.length() > 0) {
                writeLine(indentLine(INDENT + string));
            }
            buffer = new StringBuffer();
            chareventCount = 0;
        }
    }

    @Override
    public void startElement(String uri, String localName, String name, Attributes attributes)
            throws SAXException {
        writeBuffer();
        currentDepth++;

        // normalize the order of the attributes
        attributes = sortAttributes(attributes);
        StringBuffer atts = new StringBuffer();
        for (int i = 0; i < attributes.getLength(); i++) {
            atts.append(" " + attributes.getQName(i) + "=\"" + attributes.getValue(i) + "\"");
        }

        writeLine(indentLine("<" + localName + atts + ">"));

    }

    private Attributes sortAttributes(Attributes attributes) {
        SortableAttr[] sortableAttrs = new SortableAttr[attributes.getLength()];
        for (int i = 0; i < attributes.getLength(); i++) {
            sortableAttrs[i] = new SortableAttr(attributes.getQName(i), i);
        }

        Arrays.sort(sortableAttrs);

        AttributesImpl sortedAttrs = new AttributesImpl();
        for (SortableAttr sortableAttr : sortableAttrs) {
            int idx = sortableAttr.index;
            sortedAttrs.addAttribute(attributes.getURI(idx), attributes.getLocalName(idx), attributes.getQName(idx),
                    attributes.getType(idx), attributes.getValue(idx));
        }
        return sortedAttrs;
    }

    private static class SortableAttr implements Comparable {
        String key;
        int index;

        public SortableAttr(String key, int index) {
            this.key = key;
            this.index = index;
        }

        public int compareTo(Object o) {
            SortableAttr other = (SortableAttr)o;
            return key.compareTo(other.key);
        }
    }

    @Override
    public void endElement(String uri, String localName, String name) throws SAXException {
        writeBuffer();
        writeLine(indentLine("</" + localName + ">"));
        currentDepth--;
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        String string = new String(ch, start, length);
        if (string.length() > 0) {
            chareventCount++;
            // we use the trimmed string to strip whitespace
            // buffer.append(ch, start, length);
            buffer.append(string);
        }

    }

    @Override
    public void endDocument() throws SAXException {
        try {
            // just flush
            writer.flush();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    // lexical stuff

    public void comment(char[] ch, int start, int length) throws SAXException {
        String comment = new String(ch).substring(start, start + length).trim();
        writeBuffer();
        currentDepth++;
        writeLine(indentLine("<!-- "));
        writeLine(indentLine(comment));
        writeLine(indentLine("-->"));
        currentDepth--;
    }

    public void endCDATA() throws SAXException {
        // do nothing
    }

    public void endDTD() throws SAXException {
        // do nothing
    }

    public void endEntity(String name) throws SAXException {
        // do nothing
    }

    public void startCDATA() throws SAXException {
        // do nothing
    }

    public void startDTD(String name, String publicId, String systemId) throws SAXException {
        // do nothing
    }

    public void startEntity(String name) throws SAXException {
        // do nothing
    }

}