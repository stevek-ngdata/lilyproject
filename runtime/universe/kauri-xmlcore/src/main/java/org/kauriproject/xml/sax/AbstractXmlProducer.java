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

import org.xml.sax.ext.LexicalHandler;
import org.xml.sax.ContentHandler;

public class AbstractXmlProducer implements XmlProducer {
    protected ContentHandler contentHandler;
    protected LexicalHandler lexicalHandler;

    public AbstractXmlProducer() {
    }

    public AbstractXmlProducer(ContentHandler contentHandler) {
        setContentHandler(contentHandler);
    }

    public AbstractXmlProducer(ContentHandler contentHandler, LexicalHandler lexicalHandler) {
        setContentHandler(contentHandler);
        setLexicalHandler(lexicalHandler);
    }

    public AbstractXmlProducer(XmlConsumer consumer) {
        setConsumer(consumer);
    }

    public void setContentHandler(ContentHandler contentHandler) {
        this.contentHandler = contentHandler;
        if (contentHandler instanceof LexicalHandler)
            this.lexicalHandler = (LexicalHandler)contentHandler;
    }

    public void setLexicalHandler(LexicalHandler lexicalHandler) {
        this.lexicalHandler = lexicalHandler;
    }

    public void setConsumer(XmlConsumer consumer) {
        this.contentHandler = consumer;
        this.lexicalHandler = consumer;
    }
}
