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
package org.lilyproject.runtime.util.xml;

import org.xml.sax.XMLReader;
import org.xml.sax.SAXException;

import javax.xml.parsers.SAXParserFactory;
import javax.xml.parsers.ParserConfigurationException;

/**
 * Serves a similar purpose as {@link LocalDocumentBuilderFactory}.
 */
public class LocalSAXParserFactory {
    private static ThreadLocal<SAXParserFactory> LOCAL = new ThreadLocal<SAXParserFactory>() {
        protected SAXParserFactory initialValue() {
            SAXParserFactory parserFactory = SAXParserFactory.newInstance();
            parserFactory.setNamespaceAware(true);
            parserFactory.setValidating(false);
            return parserFactory;
        }
    };

    public static SAXParserFactory getSAXParserFactory() {
        return LOCAL.get();
    }

    public static XMLReader newXmlReader() throws ParserConfigurationException, SAXException {
        return getSAXParserFactory().newSAXParser().getXMLReader();
    }
}
