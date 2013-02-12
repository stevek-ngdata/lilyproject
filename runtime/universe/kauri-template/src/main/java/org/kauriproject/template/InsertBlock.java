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

import java.io.FileNotFoundException;

import javax.xml.parsers.SAXParser;

import org.kauriproject.template.el.ELFacade;
import org.kauriproject.template.el.Expression;
import org.kauriproject.template.handling.Handling;
import org.kauriproject.template.handling.HandlingInput;
import org.kauriproject.template.handling.HandlingStrategy;
import org.kauriproject.template.source.Source;
import org.kauriproject.util.io.IOUtils;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

public class InsertBlock extends TemplateBlock {

    // INSERT CONSTANTS
    public static final String SRC   = "src";
    public static final String VALUE = "value";
    public static final String MODE  = "mode";
    public static final String ENCODING  = "encoding";

    // EL
    protected ELFacade elFacade;

    protected SAXParser parser;

    public InsertBlock(ELFacade elFacade, SaxElement saxElement, SAXParser parser) {
        super(saxElement);
        this.elFacade = elFacade;
        this.parser = parser;
    }

    protected Step buildStartStep(Locator locator, NamespacesHandle namespacesHandle) {
        return new StartStep(locator);
    }

    protected Step buildEndStep(Locator locator) {
        return new EndStep(locator);
    }

    class StartStep extends Step {

        private final Expression sourceExpr;
        private final Expression valueExpr;
        private final Expression modeExpr;
        private final Expression encodingExpr;

        public StartStep(Locator locator) {
            super(locator);
            final Attributes attributes = getSaxElement().getAttributes();

            final String srcAttr = attributes.getValue(SRC);
            final String valueAttr = attributes.getValue(VALUE);

            if(srcAttr != null) {
            	sourceExpr = elFacade.createExpression(srcAttr, String.class);
            	valueExpr = null;
            } else {
                sourceExpr = null;
                valueExpr = elFacade.createExpression(valueAttr, String.class);          	
            }

            final String modeAttr = attributes.getValue(MODE);
            modeExpr = elFacade.createExpression(modeAttr, String.class);
            
            final String encodingAttr = attributes.getValue(ENCODING);
            encodingExpr = elFacade.createExpression(encodingAttr, String.class);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            final String sourceLocation = stringEval(context, sourceExpr, null);
            final String encoding = stringEval(context, encodingExpr, null);
            Source source = null;
            CharSequence valueStr = null;
            HandlingStrategy handling = null;
            HandlingInput input = null;

            try {

                // find content to insert
                if(sourceLocation != null)
            		source = context.getSourceResolver().resolve(sourceLocation, context.getBaseUri());
            	else{
            		valueStr = (CharSequence) valueExpr.evaluate(context.getTemplateContext());
            	}   

                // decide on handling mode based on @mode, media-type or default
                if (modeExpr != null) {
                    final String mode = (String)modeExpr.evaluate(context.getTemplateContext());
                    handling = Handling.fromMode(mode);  // this is silent about unknown modes
                }
                if (handling == null && source!=null) { 
                    handling = Handling.fromMediaType(source.getMediaType());
                }
                if (handling == null) { 
                    handling = Handling.TXT; // default to txt mode to be sure to include something
                }
                
                //actually insert
                input = HandlingInput.newInput(source, encoding, valueStr);
                handling.doInsert(input, result, parser); // this could NPE when parseType was still not found

            } catch (FileNotFoundException ex) {
                throw new TemplateException("Error inserting from " + (sourceLocation != null ? sourceLocation : valueStr) + " using " + handling + " : " + ex);
            } catch (Exception ex) {
                throw new TemplateException("Error parsing from " + (sourceLocation != null ? sourceLocation : valueStr) + " using " + handling + " : " + ex);
            } finally {
                if (source != null) {
                    source.release();
                }
                IOUtils.closeQuietly(input);
            }

            return super.executeAndProceed(context, result);
        }

    }

    class EndStep extends Step {

        public EndStep(Locator locator) {
            super(locator);
        }
    }
}