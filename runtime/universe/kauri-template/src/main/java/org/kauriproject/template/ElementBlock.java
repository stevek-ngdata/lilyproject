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

import org.xml.sax.Locator;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.NamespaceSupport;
import org.kauriproject.template.el.ELFacade;
import org.kauriproject.template.el.Expression;

/**
 * Implementation of the "element" template instruction.
 */
public class ElementBlock extends TemplateBlock {

    // ATTRIBUTE CONSTANTS
    public static final String NAME = "name";

    // EL
    protected ELFacade elFacade;

    public ElementBlock(ELFacade elFacade, SaxElement saxElement) {
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
        private final Expression nameExpr;
        private final NamespaceSupport namespaceSupport;

        public StartStep(Locator locator, NamespacesHandle namespacesHandle) {
            super(locator);
            namespaceSupport = namespacesHandle.snapshot();

            final Attributes attributes = getSaxElement().getAttributes();

            String nameAttr = attributes.getValue(NAME);
            if (nameAttr == null) {
                throw new TemplateException(NAME + " attribute required on " + Directive.ELEMENT.getTagName() + " instruction.");
            }
            nameExpr = elFacade.createExpression(nameAttr, String.class);
        }

        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            final String name = (String)nameExpr.evaluate(context.getTemplateContext());

            // TODO should check the name contains only characters valid in an XML name

            final String[] parsedName = namespaceSupport.processName(name, new String[3], false);

            result.startElement(parsedName[0], parsedName[1], parsedName[2], new AttributesImpl());

            context.getExecutor().execute(getCompiledNext(), getEndStep(), context, result);

            result.endElement(parsedName[0], parsedName[1], parsedName[2]);

            return getEndStep().getCompiledNext();
        }
    }

    class EndStep extends Step {
        public EndStep(Locator locator) {
            super(locator);
        }

        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            return getCompiledNext();
        }
    }
}
