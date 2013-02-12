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

import org.kauriproject.template.el.ELFacade;
import org.kauriproject.template.el.Expression;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.NamespaceSupport;

/**
 * Implementation of a "attribute" template instruction.
 */
public class AttributeBlock extends TemplateBlock {

    // ATTRIBUTE CONSTANTS
    public static final String NAME = "name";
    public static final String VALUE = "value";

    // EL
    protected ELFacade elFacade;

    public AttributeBlock(ELFacade elFacade, SaxElement saxElement) {
        super(saxElement);
        this.elFacade = elFacade;
    }

    protected Step buildStartStep(Locator locator, NamespacesHandle namespacesHandle) {
        return new StartStep(locator, namespacesHandle);
    }

    protected Step buildEndStep(Locator locator) {
        return new EndStep(locator);
    }

    class StartStep extends Step {

        private final NamespaceSupport namespaceSupport;
        private final Expression nameExpr;
        private final Expression valueExpr;

        public StartStep(Locator locator, NamespacesHandle namespacesHandle) {
            super(locator);
            Attributes attrs = getSaxElement().getAttributes();
            this.namespaceSupport = namespacesHandle.snapshot();

            final String name = attrs.getValue(NAME);
            if (name == null) {
                throw new TemplateException("Missing " + NAME + " attribute at " + getLocation());
            }
            nameExpr = elFacade.createExpression(name, String.class);

            final String value = attrs.getValue(VALUE);
            valueExpr =  elFacade.createExpression(value, String.class);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            if (valueExpr != null) {
                String value = (String)valueExpr.evaluate(context.getTemplateContext());
                String name = (String)nameExpr.evaluate(context.getTemplateContext());

                String[] parsedName = namespaceSupport.processName(name, new String[3], true);

                result.addAttribute(parsedName[0], parsedName[1], parsedName[2], "CDATA", value);

                return endStep.getCompiledNext();
            } else {
                return super.executeAndProceed(context, result);
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
