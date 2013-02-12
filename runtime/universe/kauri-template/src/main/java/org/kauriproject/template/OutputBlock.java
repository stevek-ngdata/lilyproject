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

import java.util.ArrayList;
import java.util.List;

import org.kauriproject.template.el.ELFacade;
import org.kauriproject.template.el.Expression;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

public class OutputBlock extends TemplateBlock {

    public static final String INHERIT = "inherit";

    protected ELFacade elFacade;
    protected boolean root;

    public OutputBlock(ELFacade elFacade, SaxElement saxElement, boolean root) {
        super(saxElement);
        this.elFacade = elFacade;
        this.root = root;
    }

    protected Step buildStartStep(Locator locator, NamespacesHandle namespacesHandle) {
        return new StartStep(locator);
    }

    protected Step buildEndStep(Locator locator) {
        return new EndStep(locator);
    }

    class StartStep extends Step {

        // EL
        private final List<Expression> elExpressions;
        private final Attributes attributes;

        public StartStep(Locator locator) {
            super(locator);
            attributes = getSaxElement().getAttributes();

            elExpressions = new ArrayList<Expression>();
            if (attributes.getLength() > 0) {
                for (int i = 0; i < attributes.getLength(); i++) {
                    final String expression = attributes.getValue(i);
                    elExpressions.add(elFacade.createExpression(expression, String.class));
                }
            } 
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            AttributesImpl atts = new AttributesImpl(attributes);
            if (context.isSilencing()) {
                return getEndStep().getCompiledNext();
            }

            Expression elExpression;
            for (int i = 0; i < attributes.getLength(); i++) {
                elExpression = elExpressions.get(i);
                atts.setValue(i, (String) elExpression.evaluate(context.getTemplateContext()));
            }

            context.getTemplateContext().saveContext();
            SaxElement el = getSaxElement();
            result.startElement(el.getUri(), el.getLocalName(), el.getName(), atts);

            if (root && context.getInitList().size() > 0) {
                context.setExtending(true);
                // execute the extend blocks before proceding to next
                for (InitBlock extendBlock : context.getInitList()) {
                    context.getExecutor().execute(extendBlock.getStartStep(), extendBlock.getEndStep().getCompiledNext(), context, result);
                }
                context.setExtending(false);
            }

            return super.executeAndProceed(context, result);
        }
    }

    class EndStep extends Step {

        public EndStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            SaxElement el = getSaxElement();
            result.endElement(el.getUri(), el.getLocalName(), el.getName());
            context.getTemplateContext().restoreContext();
            
            return super.executeAndProceed(context, result);
        }

    }

}
