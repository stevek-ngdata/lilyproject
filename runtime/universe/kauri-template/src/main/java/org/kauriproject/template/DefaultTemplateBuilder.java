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

import java.io.InputStream;
import java.util.*;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kauriproject.template.el.ELFacade;
import org.kauriproject.template.el.FunctionRegistry;
import org.kauriproject.template.source.Source;
import org.kauriproject.template.taglib.Taglib;
import org.kauriproject.xml.sax.NopXmlConsumer;
import org.kauriproject.xml.sax.XmlConsumer;
import org.kauriproject.util.io.IOUtils;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.LocatorImpl;
import org.xml.sax.helpers.NamespaceSupport;

/**
 * Default implementation of template builder. It's necessary to use a new instance each time you need to
 * build a template.
 */
public class DefaultTemplateBuilder implements TemplateBuilder {

    protected static Log log = LogFactory.getLog(DefaultTemplateBuilder.class);

    protected Stack<TemplateBlock> stack;
    protected Stack<Integer> pushCountStack;
    protected Map<String, MacroBlock> macroRegistry;
    protected Map<String, InheritanceBlock> inheritanceRegistry;
    protected List<InitBlock> extendList;
    protected StringBuffer charBuffer;// buffer for characters events
    protected Locator charLocator; // keep also pointer to char location
    protected NamespaceSupport namespaceSupport;
    protected NamespacesHandle namespacesHandle;
    /**
     * This stack contains an entry for each nested XML element: either true (whitespace should be stripped),
     * false (whitespace should not be stripped) or null (no change, first non-null value in the stack
     * tells what to do with whitespace).
     */
    protected List<Boolean> preserveWhitespaceStack;

    protected DocumentBlock docblock;
    protected PrefixMappingFilterBlock prefixMappingFilterBlock;
    protected Step startStep;
    protected Step endStep;
    protected Step last;

    protected Locator locator;
    protected ELFacade elFacade;
    protected SAXParser saxParser;
    protected FunctionRegistry functionRegistry;
    protected TemplateService templateService;

    private boolean silent;

    /**
     * Mapping of available taglibs with their namespaces. When the same namespace is registered twice or
     * more, the last registered taglib (of that namespace) will be used. Using the kauri template namespace
     * for a taglib is forbidden and won't have any effect.
     */
    protected Map<String, Taglib> taglibs;

    public DefaultTemplateBuilder(FunctionRegistry functionRegistry, TemplateService templateService,
            boolean silent) {
        this.stack = new Stack<TemplateBlock>();
        this.pushCountStack = new Stack<Integer>();
        this.macroRegistry = new HashMap<String, MacroBlock>();
        this.inheritanceRegistry = new HashMap<String, InheritanceBlock>();
        this.extendList = new ArrayList<InitBlock>();
        this.charBuffer = new StringBuffer();
        this.namespaceSupport = new NamespaceSupport();
        this.namespacesHandle = new NamespacesHandleImpl(namespaceSupport);
        this.functionRegistry = functionRegistry;
        this.elFacade = new ELFacade(functionRegistry);
        this.templateService = templateService;
        this.silent = silent;
        this.saxParser = createSAXParser();
        this.preserveWhitespaceStack = new ArrayList<Boolean>();
    }

    /**
     * The available {@link #taglibs} for this instance should be configured via the template module config.
     */
    public void setTaglibs(Map<String, Taglib> taglibs) {
        this.taglibs = taglibs;
    }

    protected int silencedLevel = 0;
    protected int allowedLevel = 0;

    protected boolean outputAllowed() {
        return !silent || allowedLevel > 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.kauriproject.template.TemplateBuilder#buildTemplate(org.kauriproject.template.source.Source)
     */
    public CompiledTemplate buildTemplate(Source source) {
        InputStream in = null;
        try {
            in = source.getInputStream();
            XMLReader xmlReader = saxParser.getXMLReader();
            XmlConsumer consumer = new InnerHandler(source);
            xmlReader.setContentHandler(consumer);
            xmlReader.setProperty("http://xml.org/sax/properties/lexical-handler", consumer);
            InputSource is = new InputSource(in);
            is.setSystemId(source.getReference());
            xmlReader.parse(is);
        } catch (Exception ex) {
            throw new TemplateException("Error parsing template file " + source.getReference(), ex);
        } finally {
            IOUtils.closeQuietly(in);
        }

        CompiledTemplate compiledTemplate = docblock.getCompiledTemplate();
        compiledTemplate.setLocation(source.getReference());
        compiledTemplate.setMacroRegistry(this.macroRegistry);
        compiledTemplate.setInheritanceRegistry(this.inheritanceRegistry);
        compiledTemplate.setExtendList(this.extendList);
        return compiledTemplate;
    }

    private SAXParser createSAXParser() {
        SAXParser parser;
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setNamespaceAware(true);
            factory.setFeature("http://xml.org/sax/features/namespace-prefixes", true);
            parser = factory.newSAXParser();
        } catch (SAXException saxex) {
            throw new TemplateException(saxex);
        } catch (ParserConfigurationException parsex) {
            throw new TemplateException(parsex);
        }
        return parser;
    }

    public void pushBlock(TemplateBlock block) {
        pushBlock(block, false);
    }

    public void pushBlock(TemplateBlock block, boolean isStart) {
        startStep = block.createStartStep(locator, namespacesHandle);
        if (!isStart) {
            last.setCompiledNext(startStep);
        } else if (last != null) {
            throw new TemplateException("Unexpected condition: last step is not null on start.");
        }
        last = startStep;
        stack.push(block);
    }

    public void popBlock() {
        TemplateBlock block = stack.pop();
        endStep = block.createEndStep(locator);
        last.setCompiledNext(endStep);
        last = endStep;
    }

    protected TemplateBlock findParentOnStack(Class<? extends TemplateBlock> blockClass) {
        boolean result = false;
        int index = stack.size() - 1;
        TemplateBlock block = null;
        while (!result && index >= 0) {
            block = stack.elementAt(index);
            if (block.getClass().isAssignableFrom(blockClass)) {
                result = true;
            }
            index--;
        }
        if (!result) {
            return null;
        }
        return block;
    }

    private static class NamespacesHandleImpl implements NamespacesHandle {
        private NamespaceSupport namespaceSupport;

        private NamespacesHandleImpl(NamespaceSupport namespaceSupport) {
            this.namespaceSupport = namespaceSupport;
        }

        public NamespaceSupport snapshot() {
            NamespaceSupport clone = new NamespaceSupport();
            clone.pushContext();

            Enumeration prefixes = namespaceSupport.getPrefixes();
            while (prefixes.hasMoreElements()) {
                String prefix = (String)prefixes.nextElement();
                String uri = namespaceSupport.getURI(prefix);
                clone.declarePrefix(prefix, uri);
            }

            return clone;
        }

        public NamespaceSupport get() {
            return namespaceSupport;
        }
    }

    class InnerHandler extends NopXmlConsumer {

        private boolean namespaceContextStarted;
        private Source source;
        private boolean rootElementProcessed = false;
        private Stack<String> i18nBundleContext = new Stack<String>();

        public InnerHandler(Source source) {
            this.source = source;
            namespaceContextStarted = false;
        }

        @Override
        public void setDocumentLocator(Locator saxlocator) {
            super.setDocumentLocator(saxlocator);
            locator = saxlocator;
        }

        @Override
        public void startDocument() throws SAXException {
            docblock = new DocumentBlock();
            pushBlock(docblock, true);

            prefixMappingFilterBlock = new PrefixMappingFilterBlock();
            pushBlock(prefixMappingFilterBlock);

            LexicalContextSwitchBlock lexicalContextSwitchBlock = new LexicalContextSwitchBlock(source, getCurrentI18nBundleName());
            pushBlock(lexicalContextSwitchBlock);
        }

        @Override
        public void endDocument() throws SAXException {
            popBlock(); // pop the SourceContextInit block
            popBlock(); // pop the prefix mapping filter block
            popBlock(); // pop the DocumentBlock
        }

        @Override
        public void startElement(String uri, String localName, String name, Attributes attributes)
                throws SAXException, ClassCastException {
            sendCharacters();
            if (!namespaceContextStarted)
                namespaceSupport.pushContext();
            namespaceContextStarted = false;

            boolean root = false;
            if (!rootElementProcessed) {
                rootElementProcessed = true;
                String dropPrefixMappings = attributes.getValue(NAMESPACE_KTL, "dropPrefixMappings");
                if (dropPrefixMappings != null) {
                    prefixMappingFilterBlock.addPrefixMappings(dropPrefixMappings);
                    AttributesImpl updatedAttrs = new AttributesImpl(attributes);
                    updatedAttrs.removeAttribute(updatedAttrs.getIndex(NAMESPACE_KTL, "dropPrefixMappings"));
                    attributes = updatedAttrs;
                }
                root = true;
            }

            if (silencedLevel > 0) {
                silencedLevel++;
                return;
            }

            // We only want to take care of the prefix mapping events, and remove the duplicate
            // info from the xmlns attributes (which may or may not be present, depending on parser settings)
            attributes = removeXmlnsAttrs(attributes);

            // Determine preference for whitespace treatment, if any
            Boolean preserveWhitespace = null;
            if (localName.equals(Directive.TEXT.getTagName()) && uri.equals(NAMESPACE_KTL)) {
                preserveWhitespace = Boolean.TRUE;
            } else if (attributes.getIndex("http://www.w3.org/XML/1998/namespace", "space") != -1) {
                int index = attributes.getIndex("http://www.w3.org/XML/1998/namespace", "space");
                String space = attributes.getValue(index);
                if (space.equals("preserve")) {
                    preserveWhitespace = Boolean.TRUE;
                } else if (space.equals("default")) {
                    preserveWhitespace = Boolean.FALSE;
                } else {
                    log.warn("Invalid value for xml:space attribute: " + space);
                }
                // Remove xml:space from the attributes
                AttributesImpl newAttrs = new AttributesImpl(attributes);
                newAttrs.removeAttribute(index);
                attributes = newAttrs;
            }
            preserveWhitespaceStack.add(preserveWhitespace);

            if (allowedLevel > 0) {
                allowedLevel++;
            }

            int blocksPushed = 1;

            String bundleName = attributes.getValue(NAMESPACE_I18N, "bundle");
            if (bundleName != null) {
                // Remove the bundle attribute
                AttributesImpl newAttrs = new AttributesImpl(attributes);
                newAttrs.removeAttribute(newAttrs.getIndex(NAMESPACE_I18N, "bundle"));
                attributes = newAttrs;

                i18nBundleContext.push(bundleName);
                I18nBundleSwitchBlock i18nBundleSwitch = new I18nBundleSwitchBlock(bundleName);
                pushBlock(i18nBundleSwitch);
                blocksPushed++;
            } else {
                i18nBundleContext.push(getCurrentI18nBundleName());
            }

            SaxElement saxElement = new SaxElement(uri, localName, name, attributes);

            if (root && attributes.getValue(TemplateBuilder.NAMESPACE_KTL, InheritBlock.INHERIT) != null) {
                InheritBlock inherit = new InheritBlock(elFacade, saxElement, templateService);
                pushBlock(inherit);
            } else {
                // KTL directives
                if (uri != null && uri.equals(NAMESPACE_KTL)) {
                    Directive directive = Directive.fromString(localName);
                    if (Directive.FOREACH == directive) {
                        ForEachBlock foreach = new ForEachBlock(elFacade, saxElement);
                        pushBlock(foreach);
                    } else if (Directive.IF == directive) {
                        IfBlock ifblock = new IfBlock(elFacade, saxElement);
                        pushBlock(ifblock);
                    } else if (Directive.CHOOSE == directive) {
                        ChooseBlock choose = new ChooseBlock(saxElement);
                        pushBlock(choose);
                    } else if (Directive.WHEN == directive) {
                        WhenBlock when = new WhenBlock(elFacade, saxElement, (ChooseBlock) stack.peek());
                        pushBlock(when);
                    } else if (Directive.OTHERWISE == directive) {
                        OtherwiseBlock otherwise = new OtherwiseBlock(saxElement, (ChooseBlock) stack.peek());
                        pushBlock(otherwise);
                    } else if (Directive.ATTRIBUTE == directive) {
                        AttributeBlock attblock = new AttributeBlock(elFacade, saxElement);
                        pushBlock(attblock);
                    } else if (Directive.ELEMENT == directive) {
                        ElementBlock block = new ElementBlock(elFacade, saxElement);
                        pushBlock(block);
                    } else if (Directive.INSERT == directive) {
                        InsertBlock insertblock = new InsertBlock(elFacade, saxElement, saxParser);
                        pushBlock(insertblock);
                    } else if (Directive.INCLUDE == directive) {
                        IncludeBlock includeblock = new IncludeBlock(elFacade, saxElement, templateService);
                        pushBlock(includeblock);
                    } else if (Directive.IMPORT == directive) {
                        ImportBlock importblock = new ImportBlock(elFacade, saxElement, templateService);
                        pushBlock(importblock);
                    } else if (Directive.VARIABLE == directive) {
                        VariableBlock varblock = new VariableBlock(elFacade, saxElement);
                        pushBlock(varblock);
                        if (allowedLevel == 0)
                            allowedLevel++;
                    } else if (Directive.COMMENT == directive) {
                        CommentBlock commentblock = new CommentBlock(elFacade, saxElement);
                        pushBlock(commentblock);
                    } else if (Directive.MACRO == directive) {
                        MacroBlock macroblock = new MacroBlock(saxElement);
                        pushBlock(macroblock);
                        LexicalContextSwitchBlock lexicalContextSwitchBlock = new LexicalContextSwitchBlock(source, getCurrentI18nBundleName());
                        pushBlock(lexicalContextSwitchBlock);
                        blocksPushed++;
                        if (macroRegistry.containsKey(macroblock.getName())) {
                            throw new TemplateException("A macro with name " + macroblock.getName()
                                    + " is already defined, location "
                                    + macroblock.getStartStep().getLocation());
                        }
                        macroRegistry.put(macroblock.getName(), macroblock);
                        if (allowedLevel == 0)
                            allowedLevel++;
                    } else if (Directive.PARAMETER == directive) {
                        // Find the parent macro or callMacro block
                        TemplateBlock parentBlock = null;
                        if (stack.get(stack.size() - 1) instanceof CallMacroBlock)
                            parentBlock = stack.get(stack.size() - 1);
                        // In case of define macro, the block is two levels deep
                        if (stack.size() > 1 && stack.get(stack.size() - 2) instanceof MacroBlock)
                            parentBlock = stack.get(stack.size() - 2);

                        boolean overwrite = parentBlock instanceof CallMacroBlock;
                        ParameterBlock parameterblock = new ParameterBlock(overwrite, elFacade, saxElement);

                        if (parentBlock == null) {
                            throw new TemplateException(
                                    "Parameter must appear as a direct child in a macro or callMacro element, location "
                                            + parameterblock.getStartStep().getLocation());
                        }

                        pushBlock(parameterblock);
                    } else if (Directive.CALLMACRO == directive) {
                        CallMacroBlock callmacroblock = new CallMacroBlock(saxElement);
                        pushBlock(callmacroblock);
                    } else if (Directive.BLOCK == directive) {
                        InheritanceBlock inheritblock = new InheritanceBlock(saxElement);
                        pushBlock(inheritblock);
                        LexicalContextSwitchBlock lexicalContextSwitchBlock = new LexicalContextSwitchBlock(source, getCurrentI18nBundleName());
                        pushBlock(lexicalContextSwitchBlock);
                        blocksPushed++;
                        if (inheritanceRegistry.containsKey(inheritblock.getName())) {
                            throw new TemplateException("A block with name " + inheritblock.getName()
                                    + " is already defined, location "
                                    + inheritblock.getStartStep().getLocation());
                        }
                        inheritanceRegistry.put(inheritblock.getName(), inheritblock);
                    } else if (Directive.SUPERBLOCK == directive) {
                        InheritanceBlock parent = (InheritanceBlock) findParentOnStack(InheritanceBlock.class);
                        SuperBlock superblock = new SuperBlock(saxElement, parent);
                        if (parent == null) {
                            throw new TemplateException(
                                    "A call to a superBlock is only possible from within an overriding block, location "
                                            + superblock.getStartStep().getLocation());
                        }
                        pushBlock(superblock);
                    } else if (Directive.INIT == directive) {
                        InitBlock initblock = new InitBlock(saxElement);
                        pushBlock(initblock);
                        LexicalContextSwitchBlock lexicalContextSwitchBlock = new LexicalContextSwitchBlock(source, i18nBundleContext.peek());
                        pushBlock(lexicalContextSwitchBlock);
                        blocksPushed++;
                        extendList.add(initblock);
                    } else if (Directive.PROTECT == directive) {
                        ProtectBlock protectblock = new ProtectBlock(elFacade, saxElement);
                        pushBlock(protectblock);
                    } else if (Directive.TEXT == directive) {
                        // don't push a block
                    } else {
                        log.warn("Unsupported KTL directive: " + localName);
                        // TODO: maybe we should provide an errorblock in this case ?
                        OutputBlock outblock = new OutputBlock(elFacade, saxElement, root);
                        pushBlock(outblock);
                    }
                } else if (taglibs != null && taglibs.containsKey(uri)) {
                    taglibs.get(uri).handleStart(DefaultTemplateBuilder.this, elFacade, locator, localName,
                            name, attributes);
                } else if (outputAllowed()) { // "ordinary" start elements
                    OutputBlock outblock = new OutputBlock(elFacade, saxElement, root);
                    pushBlock(outblock);
                } else {
                    silencedLevel++;
                }
            }
            if (silencedLevel == 0)
                pushCountStack.push(blocksPushed);
        }

        private String getCurrentI18nBundleName() {
            if (!i18nBundleContext.empty()) {
                return i18nBundleContext.peek();
            } else {
                return null;
            }
        }

        public Attributes removeXmlnsAttrs(Attributes attrs) {
            AttributesImpl newAttrs = null;
            for (int i = 0; i < attrs.getLength(); i++) {
                String qName = attrs.getQName(i);
                if (qName.equals("xmlns") || qName.startsWith("xmlns:")) {
                    if (newAttrs == null) {
                        // init, add all attributes before current one
                        newAttrs = new AttributesImpl();
                        for (int j = 0; j < i; j++) {
                            newAttrs.addAttribute(attrs.getURI(j), attrs.getLocalName(j), attrs.getQName(j),
                                    attrs.getType(j), attrs.getValue(j));
                        }
                    }
                } else if (newAttrs != null) {
                    newAttrs.addAttribute(attrs.getURI(i), attrs.getLocalName(i), attrs.getQName(i), attrs
                            .getType(i), attrs.getValue(i));
                }
            }
            return newAttrs != null ? newAttrs : attrs;
        }

        @Override
        public void endElement(String uri, String localName, String name) throws SAXException,
                ClassCastException {

            // revert to previous context
            namespaceSupport.popContext();

            if (silencedLevel > 0) {
                silencedLevel--;
            } else if (localName.equals(Directive.TEXT.getTagName()) && uri.equals(NAMESPACE_KTL)) {
                // The text instruction does not have a runtime block
                sendCharacters();
            } else {
                sendCharacters();

                // we just pop, counting on the fact that we handle balanced xml
                int blocksPushed = pushCountStack.pop();
                for (int i = 0; i < blocksPushed; i++)
                    popBlock();

                if (allowedLevel > 0)
                    allowedLevel--;

                i18nBundleContext.pop();
            }

            // Popping the preserve-whitespace-stack should be done after sendCharacters()
            preserveWhitespaceStack.remove(preserveWhitespaceStack.size() - 1);
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            if (silencedLevel == 0) {
                // We have to buffer the characters events, because the parser
                // may split up a single textblock in multiple events.
                charBuffer.append(ch, start, length);
                charLocator = new LocatorImpl(locator);
            }
        }

        private void sendCharacters() {
            if (charBuffer.length() > 0) {
                boolean ws = isWhitespace(charBuffer);
                if (!ws || preserveWhitespace()) {
                    String chars = charBuffer.toString();
                    TextStep text = new TextStep(elFacade, charLocator, chars.toCharArray(), 0, chars.length());
                    last.setCompiledNext(text);
                    last = text;
                }
                charBuffer = new StringBuffer();
            }
        }

        private boolean isWhitespace(StringBuffer buffer) {
            // We follow the same rules as for XSLT to decide if a character is whitespace
            for (int i = 0; i < buffer.length(); i++) {
                char c = buffer.charAt(i);
                if (c != 0x000D && c != 0x000A && c != 0x0009 && c != 0x020)
                    return false;
            }
            return true;
        }

        private boolean preserveWhitespace() {
            int size = preserveWhitespaceStack.size();
            for (int i = size - 1; i >= 0; i--) {
                if (preserveWhitespaceStack.get(i) != null) {
                    return preserveWhitespaceStack.get(i);
                }
            }
            // By default, pure-whitespace text between tags is stripped
            return false;
        }

        @Override
        public void startPrefixMapping(String prefix, String uri) throws SAXException {
            if (!namespaceContextStarted) {
                namespaceSupport.pushContext();
                namespaceContextStarted = true;
            }

            namespaceSupport.declarePrefix(prefix, uri);

            StartPrefixMappingStep step = new StartPrefixMappingStep(charLocator, prefix, uri);
            last.setCompiledNext(step);
            last = step;
        }

        @Override
        public void endPrefixMapping(String prefix) throws SAXException {
            EndPrefixMappingStep step = new EndPrefixMappingStep(charLocator, prefix);
            last.setCompiledNext(step);
            last = step;
        }
    }
}
