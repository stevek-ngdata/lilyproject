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
import org.xml.sax.ContentHandler;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.apache.commons.lang.ArrayUtils;
import org.kauriproject.xml.dom.DOMStreamer;

import java.util.Arrays;
import java.util.Collection;

public class Saxizer {
    /**
     * Recognizes some types of objects as being XML, and streams them as such.
     *
     * <p>Strings or non-recognized objects are streamed as a characters event.
     *
     * <p>Recognized objects include:
     * <ul>
     * <li>everything implementing the {@link Saxable} interface
     * <li>W3C DOM Nodes and NodeLists
     * <li>Collections and arrays: the individual elements are streamed.
     * </ul>
     *
     * <p>This class never parses XML, it only streams existing object
     * structures as XML.
     *
     */
    public static void toSax(Object value, ContentHandler contentHandler) throws SAXException {
        toSax(value, contentHandler, true);
    }

    private static void toSax(Object value, ContentHandler contentHandler, boolean expandLists) throws SAXException {
        if (value == null) {
            contentHandler.characters(ArrayUtils.EMPTY_CHAR_ARRAY, 0, 0);
        } else if (value instanceof String) {
            String string = (String)value;
            contentHandler.characters(string.toCharArray(), 0, string.length());
        } else if (value instanceof Saxable) {
            ((Saxable)value).toSAX(new EmbedXmlFilter(contentHandler));
        } else if (value instanceof Node) {
            streamDOM((Node)value, contentHandler);
        } else if (value instanceof NodeList) {
            NodeList nodeList = (NodeList)value;
            for (int i = 0; i < nodeList.getLength(); i++) {
                streamDOM(nodeList.item(i), contentHandler);
            }
        } else if (expandLists && value instanceof Collection) {
            for (Object entry : (Collection)value) {
                toSax(entry, contentHandler, false);
            }
        } else if (expandLists && value instanceof Object[]) {
             for (Object entry : (Object[])value) {
                 toSax(entry, contentHandler, false);
             }
        } else {
            String string = String.valueOf(value);
            contentHandler.characters(string.toCharArray(), 0, string.length());
        }
    }

    private static void streamDOM(Node node, ContentHandler contentHandler) throws SAXException {
        new DOMStreamer(new EmbedXmlFilter(contentHandler)).stream(node);
    }
}
