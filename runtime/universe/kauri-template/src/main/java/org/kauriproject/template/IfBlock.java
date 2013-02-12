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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kauriproject.template.el.ELFacade;
import org.kauriproject.template.el.Expression;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;

/**
 * Implementation of a "if" template instruction.
 */
public class IfBlock extends TemplateBlock {

    protected static Log log = LogFactory.getLog(IfBlock.class);

    // IF CONSTANTS
    public static final String TEST = "test";

    // EL
    protected ELFacade elFacade;

    public IfBlock(ELFacade elFacade, SaxElement saxElement) {
        super(saxElement);
        this.elFacade = elFacade;
    }

    protected Step buildStartStep(Locator locator, NamespacesHandle namespacesHandle) {
        return new StartStep(locator);
    }

    protected Step buildEndStep(Locator locator) {
        return new EndStep(locator);
    }

    class StartStep extends Step {

        private final Expression testExpr;

        public StartStep(Locator locator) {
            super(locator);

            Attributes attributes = getSaxElement().getAttributes();

            String testAttr = attributes.getValue(TEST);
            if (testAttr == null)
                throw new TemplateException(Directive.IF.getTagName() +
                        " instruction is missing required test attribute. Location: " + getLocation());

            testExpr = elFacade.createExpression(testAttr, Boolean.class);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) {

            final boolean test = (Boolean) testExpr.evaluate(context.getTemplateContext());

            if (test) {
                return getCompiledNext();
            } else {
                return endStep.getCompiledNext();
            }

        }
    }

    class EndStep extends Step {
        public EndStep(Locator locator) {
            super(locator);
        }
    }

}
