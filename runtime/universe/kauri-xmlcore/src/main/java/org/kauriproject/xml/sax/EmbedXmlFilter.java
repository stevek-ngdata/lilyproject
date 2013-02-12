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

import org.xml.sax.SAXException;
import org.xml.sax.ContentHandler;
import org.xml.sax.ext.LexicalHandler;

/**
 * Makes a SAX event stream suitable for embedding (insertion) into
 * another SAX event stream, by dropping things like start and end
 * document events. 
 */
public class EmbedXmlFilter extends XmlFilter {
    private boolean inDTD;

    public EmbedXmlFilter(ContentHandler contentHandler) {
        super(contentHandler);
    }

    public EmbedXmlFilter(ContentHandler contentHandler, LexicalHandler lexicalHandler) {
        super(contentHandler, lexicalHandler);
    }

    public EmbedXmlFilter(XmlConsumer consumer) {
        super(consumer);
    }

    public void startDocument() throws SAXException {
        // don't forward this event
    }

    public void endDocument() throws SAXException {
        // don't forward this event
    }

    public void startDTD(String name, String publicId, String systemId) throws SAXException {
        inDTD = true;
        // don't forward this event
    }

    public void endDTD() throws SAXException {
        inDTD = false;
        // don't forward this event
    }

    public void comment(char[] ch, int start, int length) throws SAXException {
        if (!inDTD)
            super.comment(ch, start, length);
    }
}
