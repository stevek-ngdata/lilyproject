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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.xml.parsers.SAXParser;

import org.kauriproject.template.KauriSaxHandler.OutputFormat;
import org.kauriproject.template.el.ELFacade;
import org.kauriproject.template.el.Expression;
import org.kauriproject.template.handling.Handling;
import org.kauriproject.template.handling.HandlingInput;
import org.kauriproject.template.source.Source;
import org.kauriproject.util.io.IOUtils;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

/**
 * Implementation of a "variable" template instruction.
 */
public class VariableBlock extends TemplateBlock {

    // VARIABLE CONSTANTS
    private static final String NAME = "name";
    private static final String VALUE = "value";
    private static final String SRC = "src";
    private static final String OVERWRITE = "overwrite";
    private static final String ACCEPT = "accept";
    private static final String MODE = "mode";

    // EL support
    private ELFacade elFacade;
    
    // XML support
    protected SAXParser parser;


    public VariableBlock(ELFacade elFacade, SaxElement saxElement) {
        super(saxElement);
        this.elFacade = elFacade;
    }

    protected Step buildStartStep(Locator locator, NamespacesHandle namespacesHandle) {
        return new StartStep(locator);
    }

    protected Step buildEndStep(Locator locator) {
        return new EndStep(locator);
    }

    private static final Object NO_VALUE = new Object();

    class StartStep extends Step {

        // variable stuff
        private final String name;
        private final Expression valueExpr;
        private final Expression srcExpr;
        private final Expression acceptExpr;
        private final Expression modeExpr;
        private final boolean overwrite;

        // compiledNext: first following instruction
        // runtimeNext may be endStep.compiledNext

        public StartStep(Locator locator) {
            super(locator);            
            Attributes attributes = getSaxElement().getAttributes();

            name = attributes.getValue(NAME);

            final String valAttr = attributes.getValue(VALUE);
            valueExpr = elFacade.createExpression(valAttr, Object.class);

            final String srcAttr = attributes.getValue(SRC);
            srcExpr = elFacade.createExpression(srcAttr, String.class);

            final String acceptAttr = attributes.getValue(ACCEPT);
            acceptExpr = elFacade.createExpression(acceptAttr, String.class);

            final String modeAttr = attributes.getValue(MODE);
            modeExpr = elFacade.createExpression(modeAttr, String.class);

            overwrite = !"false".equalsIgnoreCase(attributes.getValue(OVERWRITE));
        }

        
        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            TemplateContext tmplContext = context.getTemplateContext();
            if (overwrite || !tmplContext.containsKey(name)) {
                Object value = NO_VALUE;  // use our own null to differentiate from null
                if (valueExpr != null) {
                    value = valueExpr.evaluate(context.getTemplateContext());
                } else {
                    // use element body as value
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    KauriSaxHandler ksh = new KauriSaxHandler(bos, OutputFormat.TEXT, "UTF-8", true, true);
                    TemplateResult buffer = new TemplateResultImpl(ksh);
                    buffer.startDocument();
                    Step next = this.getCompiledNext();

                    final boolean silencing = context.isSilencing();
                    context.setSilencing(false);    //ignore silencing mode for grabbing value
                    while (next != endStep) {
                        next = next.executeAndProceed(context, buffer);
                    }
                    context.setSilencing(silencing);//restore silencing mode

                    buffer.endDocument();
                    buffer.flush();
                    try {
                        bos.flush();
                        final String content = bos.toString("UTF-8");
                        if (content != null && content.length() > 0)
                            value = content; 
                    } catch (IOException ex) {
                        throw new TemplateException("Error building variable value.", ex);
                    }
                }

                // value retrieved from @value or body can be overwritten by loaded data from @src
                value = overideFromSrc(context, value);
                
                value = (value == NO_VALUE) ? null : value; // switch tracking NO_VALUE back to null before setting it in the context
                tmplContext.put(name, value);
            }

            return endStep.getCompiledNext();
        }

        private Object overideFromSrc(final ExecutionContext context, final Object value) {
            final String src = stringEval(context, srcExpr, null);
            final String accept = stringEval(context, acceptExpr, null);

            Source source = null;
            HandlingInput input = null;
            Handling handling = null;
            try {
                source = (src == null) ? null : context.getSourceResolver().resolve(src, context.getBaseUri(), accept);

                // decide on handling mode based on @mode, media-type or default
                if (modeExpr != null) {
                    final String mode = (String)modeExpr.evaluate(context.getTemplateContext());
                    handling = Handling.fromMode(mode);  // this is silent about unknown modes
                }
                
                if (source != null) {
                    input = HandlingInput.newInput(source);
                    if (handling == null) { 
                        handling = Handling.fromMediaType(source.getMediaType());
                    }
                } else if (value instanceof CharSequence) {
                    input = HandlingInput.newInput(null, null, (CharSequence)value);
                } else
                    return value;
                
                if (handling == null) { 
                    handling = Handling.TXT; // default to txt mode to be sure to include something
                }
                return handling.parseToObject(input);                
            } catch (Throwable e) {
                if (value == NO_VALUE)
                    throw new TemplateException("Some error occured while loading data for variable " + name + " from URI " + src, e);    
                // else
                return value;
            } finally {
                if (source != null) {
                    source.release();
                }
                IOUtils.closeQuietly(input);
            }
        }
    }

    class EndStep extends Step {

        // compiledNext: first following instruction after block
        // runtimeNext idem

        public EndStep(Locator locator) {
            super(locator);
        }

    }

}
