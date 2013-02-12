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
package org.kauriproject.template;

import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.kauriproject.template.el.ELFacade;
import org.kauriproject.template.el.Expression;

import java.util.List;
import java.util.Map;
import java.util.LinkedList;

public class InheritBlock  extends TemplateBlock {

    public static final String INHERIT = "inherit";

    protected ELFacade elFacade;
    protected TemplateService templateService;
    protected boolean root;

    public InheritBlock(ELFacade elFacade, SaxElement saxElement, TemplateService templateService) {
        super(saxElement);
        this.elFacade = elFacade;
        this.templateService = templateService;
    }

    protected Step buildStartStep(Locator locator, NamespacesHandle namespacesHandle) {
        return new StartStep(locator);
    }

    protected Step buildEndStep(Locator locator) {
        return new EndStep(locator);
    }

    class StartStep extends Step {

        private final Expression inheritExpr;

        public StartStep(Locator locator) {
            super(locator);
            Attributes attributes = getSaxElement().getAttributes();

            String inheritAttr = attributes.getValue(TemplateBuilder.NAMESPACE_KTL, INHERIT);
            inheritExpr = elFacade.createExpression(inheritAttr, String.class);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            String sourceLocation = (String)inheritExpr.evaluate(context.getTemplateContext());
            sourceLocation = context.getSourceResolver().toAbsolutePath(sourceLocation, context.getBaseUri());

            // inheritance => lookup base
            CompiledTemplate baseTemplate;
            try {
                // ask service to get compiled template for sourcelocation
                // this may trigger a build of the base template
                baseTemplate = templateService.buildTemplate(sourceLocation, context);
            } catch (Exception ex) {
                throw new TemplateException("Error parsing inherited template specified at location " + getLocation(), ex);
            }

            // merge inheritance mappings
            Map<String, List<InheritanceBlock>> inheritanceChain = context.getInheritanceChain();
            Map<String, InheritanceBlock> baseRegistry = baseTemplate.getInheritanceRegistry();
            for (String block : baseRegistry.keySet()) {
                if (!inheritanceChain.containsKey(block)) {
                    List<InheritanceBlock> chain = new LinkedList<InheritanceBlock>();
                    chain.add(baseRegistry.get(block));
                    inheritanceChain.put(block, chain);
                } else {
                    inheritanceChain.get(block).add(baseRegistry.get(block));
                }
            }

            context.inheritInc();
            context.getExecutor().execute(baseTemplate, null, context, result);
            context.inheritDecr();

            return getEndStep().getCompiledNext();
        }

    }

    class EndStep extends Step {

        public EndStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            return super.executeAndProceed(context, result);
        }

    }

}

