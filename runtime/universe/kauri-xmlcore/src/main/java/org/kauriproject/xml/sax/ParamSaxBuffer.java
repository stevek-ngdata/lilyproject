/*
 * Copyright 2009 Outerthought bvba and Schaubroeck nv
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

import org.xml.sax.SAXException;
import org.xml.sax.Locator;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.Writer;
import java.io.IOException;

/**
 * Extension of SaxBuffer, where character-events can contain {}-encoded
 * parameters which can be subsituted during streaming out the SaxBuffer.
 *
 * <p>Note: when not pushing a complete document into this, but rather a fragment,
 * make sure to call the {@link #flush} method at the end.
 */
public class ParamSaxBuffer extends SaxBuffer {
    private StringBuilder buffer = new StringBuilder();
    private static Pattern PARAM_PATTERN = Pattern.compile("[\\\\]?\\{([^\\}]+)\\}");

    public static interface ParamResolver {
        Object resolve(String name);
    }

    public void toSAX(ContentHandler contentHandler, ParamResolver paramResolver) throws SAXException {
        for (SaxBit saxbit : saxbits) {
            if (saxbit instanceof Parameter) {
                ((Parameter)saxbit).send(contentHandler, paramResolver);
            } else {
                saxbit.send(contentHandler);
            }
        }
    }

    public String toString(ParamResolver paramResolver) {
        final StringBuilder value = new StringBuilder();
        for (SaxBit saxbit : saxbits) {
            if (saxbit instanceof Parameter) {
                ((Parameter)saxbit).toString(value, paramResolver);
            } else if (saxbit instanceof Characters) {
                ((Characters)saxbit).toString(value);
            }
        }
        return value.toString();
    }

    @Override
    public String toString() {
        final StringBuilder value = new StringBuilder();
        for (SaxBit saxbit : saxbits) {
            if (saxbit instanceof Parameter) {
                ((Parameter)saxbit).toString(value);
            } else if (saxbit instanceof Characters) {
                ((Characters)saxbit).toString(value);
            }
        }
        return value.toString();
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        buffer.append(ch, start, length);
        //super.characters(ch, start, length);
    }

    public void flush() {
        if (buffer.length() > 0) {
            String input = buffer.toString();

            Matcher matcher = PARAM_PATTERN.matcher(input);
            int lastMatchEnd = 0;

            while (matcher.find()) {
                String ref = matcher.group(1);

                // backslash is the escape character
                if (matcher.start() > 0 && input.charAt(matcher.start()) == '\\') {
                    String literal = input.substring(lastMatchEnd, matcher.start()) + input.substring(matcher.start() + 1, matcher.end());
                    addBit(new Characters(literal.toCharArray(), 0, literal.length()));
                    lastMatchEnd = matcher.end();
                    continue;
                }

                // output remaining literal content
                if (matcher.start() > lastMatchEnd) {
                    String literal = input.substring(lastMatchEnd, matcher.start());
                    addBit(new Characters(literal.toCharArray(), 0, literal.length()));
                }

                addBit(new Parameter(ref));
                lastMatchEnd = matcher.end();
            }

            // append tail
            if (lastMatchEnd < input.length()) {
                String literal = input.substring(lastMatchEnd);
                addBit(new Characters(literal.toCharArray(), 0, literal.length()));
            }

            buffer.setLength(0);
        }
    }

    @Override
    public void setDocumentLocator(Locator locator) {
        flush();
        super.setDocumentLocator(locator);
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        flush();
        super.ignorableWhitespace(ch, start, length);
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        flush();
        super.processingInstruction(target, data);
    }

    @Override
    public void startDocument() throws SAXException {
        flush();
        super.startDocument();
    }

    @Override
    public void startElement(String namespaceURI, String localName, String qName, Attributes atts) throws SAXException {
        flush();
        super.startElement(namespaceURI, localName, qName, atts);
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        flush();
        super.endPrefixMapping(prefix);
    }

    @Override
    public void endElement(String namespaceURI, String localName, String qName) throws SAXException {
        flush();
        super.endElement(namespaceURI, localName, qName);
    }

    @Override
    public void endDocument() throws SAXException {
        flush();
        super.endDocument();
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        flush();
        super.startPrefixMapping(prefix, uri);
    }

    @Override
    public void endCDATA() throws SAXException {
        flush();
        super.endCDATA();
    }

    @Override
    public void comment(char[] ch, int start, int length) throws SAXException {
        flush();
        super.comment(ch, start, length);
    }

    @Override
    public void startEntity(String name) throws SAXException {
        flush();
        super.startEntity(name);
    }

    @Override
    public void endDTD() throws SAXException {
        flush();
        super.endDTD();
    }

    @Override
    public void startDTD(String name, String publicId, String systemId) throws SAXException {
        flush();
        super.startDTD(name, publicId, systemId);
    }

    @Override
    public void startCDATA() throws SAXException {
        flush();
        super.startCDATA();
    }

    @Override
    public void endEntity(String name) throws SAXException {
        flush();
        super.endEntity(name);
    }

    static final class Parameter implements SaxBit {
        private final String name;

        public Parameter(String name) {
            this.name = name;
        }

        public void send(ContentHandler contentHandler) throws SAXException {
            String string = "{" + name + "}";
            contentHandler.characters(string.toCharArray(), 0, string.length());
        }

        public void send(ContentHandler contentHandler, ParamResolver paramResolver) throws SAXException {
            Object value = paramResolver.resolve(name);
            if (value != null) {
                Saxizer.toSax(value, contentHandler);
            }
        }

        public void toString(StringBuilder result, ParamResolver paramResolver) {
            Object value = paramResolver.resolve(name);
            if (value != null) {
                result.append(String.valueOf(value));
            }
        }

        public void toString(StringBuilder result) {
            result.append("{").append(name).append("}");
        }

        public void dump(Writer writer) throws IOException {
            writer.write("[Parameter] name=" + name);
        }
    }
}
