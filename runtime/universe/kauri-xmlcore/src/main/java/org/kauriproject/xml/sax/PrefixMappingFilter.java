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
import org.xml.sax.Attributes;
import org.xml.sax.helpers.AttributesImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

/**
 * First cut at a namespace prefix supresser. The idea is that someone wants
 * to remove prefix-NS mappings which are not used in the resulting XML stream,
 * mainly to avoid that a serialized XML document contains unused 'xmlns' attributes.
 *
 * <p>We rely on the user providing a list of such mappings, since otherwise
 * we can't implement this as a streaming filter. Besides, prefixes might also be used
 * in content, which we can't know about.
 *
 * <p>If the actual SAX stream still contains elements or attributes using
 * removed mappings, an error is logged but the mapping will still be gone.
 *
 * <p>It are specific prefix-namespace associations which are removed. The
 * same prefix bound to another namespace, or the same namespace bound to
 * another prefix, will be left untouched.
 *
 * <p>If the user requests to remove a prefix mapping which is still in use
 * by elements or attributes, an error will be logged and the namespace will
 * be dropped, rather than trying to re-insert the namespace mapping dynamically.
 *
 * <p>This filter is intended to handle SAX-streams who result from programmatic
 * generation and filtering, where often the prefix mappings might have become
 * a bit of a mess due to removal of elements, embedding of 'blurbs of SAX'
 * from other locations, etc. So there might be multiple prefix mappings for
 * the same prefix or the same namespace per element etc.
 */
public class PrefixMappingFilter extends XmlFilter {
    private List<PrefixAndUri> prefixAndUris;
    private List<PrefixContext> prefixContexts = new ArrayList<PrefixContext>();
    private boolean dropAllMode = false;
    private int prefixContextPosition = -1;

    private Log log = LogFactory.getLog(getClass());

    /**
     *
     * @param prefixAndUris the list might contain the same prefix bound to different
     *                      namespaces, and the same namespace bound to different prefixes.
     */
    public PrefixMappingFilter(List<PrefixAndUri> prefixAndUris, boolean dropAllMode) {
        this.prefixAndUris = prefixAndUris;
        this.dropAllMode = dropAllMode;
        pushPrefixContext();
    }

    @Override
    public void startDocument() throws SAXException {
        pushPrefixContext();
        super.startDocument();
    }

    @Override
    public void endDocument() throws SAXException {
        super.endDocument();
        popPrefixContext();
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        if (needsRemoval(prefix, uri)) {
            getPrefixContext().pushPrefix(prefix, uri, true);
        } else {
            getPrefixContext().pushPrefix(prefix, uri, false);
            super.startPrefixMapping(prefix, uri);
        }
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        UriEntry uriEntry = getPrefixContext().popPrefix(prefix);
        if (!uriEntry.drop) {
            super.endPrefixMapping(prefix);
        }
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        pushPrefixContext();

        boolean removeNamespace = false;
        if (!uri.equals("")) {
            int colonPos = qName.indexOf(":");
            String prefix = colonPos != -1 ? qName.substring(colonPos + 1) : null;
            if (needsRemoval(prefix, uri)) {
                // A prefix-namespace combination that we were asked to remove is still in use.
                // Rather than trying to fix this by re-introducing the namespace mapping,
                // just drop the namespace and report an error.
                removeNamespace = true;
                if (log.isErrorEnabled()) {
                    log.error("It was requested to remove the namespace mapping (" + prefix + ", " + uri + ") "
                            + "but encountered an element which still uses it: " + qName
                            + ". The element will be outputted without the namespace. It is recommended you "
                            + "fix this situation, if you want to remove namespace use appropriate namespace "
                            + "removal functionality instead of relying on namespace prefix supressing.");
                }
            }
        }

        atts = processAttributes(atts);
        if (removeNamespace) {
            super.startElement("", localName, localName, atts);
        } else {
            super.startElement(uri, localName, qName, atts);
        }
    }

    private Attributes processAttributes(Attributes attrs) {
        // Check if any attributes use a removed prefix-NS mapping.

        AttributesImpl newAttrs = null;

        for (int i = 0; i < attrs.getLength(); i++) {
            String uri = attrs.getURI(i);
            boolean processed = false;

            if (!uri.equals("")) {
                String qName = attrs.getQName(i);
                String prefix = getPrefix(qName);

                if (needsRemoval(prefix, uri)) {
                    if (newAttrs == null)
                        newAttrs = copyAttributes(attrs, i - 1);

                    processed = true;

                    boolean otherAttrWithSameLocalName = false;
                    String localName = attrs.getLocalName(i);
                    for (int k = 0; k < attrs.getLength(); k++) {                        
                        if (attrs.getLocalName(k).equals(localName) && !attrs.getURI(k).equals(uri)) {
                            otherAttrWithSameLocalName = true;
                            break;
                        }
                    }

                    if (!otherAttrWithSameLocalName) {
                        newAttrs.addAttribute("", attrs.getLocalName(i), attrs.getLocalName(i), attrs.getType(i), attrs.getValue(i));
                        log.error("It was requested to remove the namespace mapping (" + prefix + ", " + uri + ") "
                                + "but encountered an attribute which still uses it: " + qName
                                + ". The attribute namespace will be removed. It is recommended you "
                                + "fix this situation, if you want to remove namespace use appropriate namespace "
                                + "removal functionality instead of relying on namespace prefix supressing.");
                    } else {
                        if (log.isErrorEnabled()) {
                            log.error("It was requested to remove the namespace mapping (" + prefix + ", " + uri + ") "
                                    + "but encountered an attribute which still uses it: " + qName
                                    + ". The attribute will be removed from the output since there's already "
                                    + "another attribute with the same local name. It is recommended you "
                                    + "fix this situation.");
                        }
                    }
                }
            }

            if (!processed && newAttrs != null) {
                newAttrs.addAttribute(attrs.getURI(i), attrs.getLocalName(i), attrs.getQName(i), attrs.getType(i), attrs.getValue(i));
            }
        }

        return newAttrs != null ? newAttrs : attrs;
    }

    private AttributesImpl copyAttributes(Attributes atts, int upToInclude) {
        AttributesImpl newAttrs = new AttributesImpl();
        for (int i = 0; i <= upToInclude; i++) {
            newAttrs.addAttribute(atts.getURI(i), atts.getLocalName(i), atts.getQName(i), atts.getType(i), atts.getValue(i));
        }
        return newAttrs;
    }

    private String getPrefix(String qName) {
        int colonPos = qName.indexOf(":");
        return colonPos != -1 ? qName.substring(0, colonPos) : null;
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        boolean removeNS = popPrefixContext();
        if (removeNS) {
            super.endElement("", localName, localName);
        } else {
            super.endElement(uri, localName, qName);
        }
    }

    private boolean needsRemoval(String prefix, String uri) {
        if (dropAllMode)
            return true;
        
        for (PrefixAndUri prefixAndUri : prefixAndUris) {
            if (prefixAndUri.prefix.equals(prefix) && prefixAndUri.uri.equals(uri)) {
                return true;
            }
        }
        return false;
    }

    private void pushPrefixContext() {
        prefixContextPosition++;

        if (prefixContexts.size() == prefixContextPosition) {
            prefixContexts.add(new PrefixContext());
        }
    }

    private boolean popPrefixContext() {
        PrefixContext context = prefixContexts.get(prefixContextPosition);
        boolean dropNS = context.dropNamespace;
        context.recycle();
        prefixContextPosition--;
        return dropNS;
    }

    private PrefixContext getPrefixContext() {
        return prefixContexts.get(prefixContextPosition);
    }

    private static class PrefixContext {
        Map<String, List<UriEntry>> prefixes = new HashMap<String, List<UriEntry>>();
        boolean dropNamespace;

        public void pushPrefix(String prefix, String uri, boolean drop) {
            List<UriEntry> uriEntries = prefixes.get(prefix);
            if (uriEntries == null) {
                uriEntries = new ArrayList<UriEntry>();
                prefixes.put(prefix, uriEntries);
            }
            uriEntries.add(new UriEntry(uri, drop));
        }

        public UriEntry popPrefix(String prefix) {
            List<UriEntry> uriEntries = prefixes.get(prefix);
            if (uriEntries == null)
                return null;

            UriEntry uriEntry = uriEntries.remove(uriEntries.size() - 1);
            return uriEntry;
        }

        void recycle() {
            prefixes.clear();
            dropNamespace = false;
        }
    }

    private static class UriEntry {
        boolean drop;
        String uri;

        public UriEntry(String uri, boolean drop) {
            this.uri = uri;
            this.drop = drop;
        }
    }

    public static class PrefixAndUri {
        private String prefix;
        private String uri;

        /**
         *
         * @param prefix empty string indicates default namespace
         */
        public PrefixAndUri(String prefix, String uri) {
            this.prefix = prefix;
            this.uri = uri;
        }
    }

}
