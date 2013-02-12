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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.xml.sax.Locator;
import org.xml.sax.SAXException;

public class DocumentBlock extends TemplateBlock {

    public DocumentBlock() {
        super();
    }

    protected Step buildStartStep(Locator locator, NamespacesHandle namespacesHandle) {
        return new StartStep(locator);
    }

    protected Step buildEndStep(Locator locator) {
        return new EndStep(locator);
    }

    public CompiledTemplate getCompiledTemplate() {
        return (CompiledTemplate)startStep;
    }

    class StartStep extends CompiledTemplate {

        public StartStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            boolean include = context.getIncludeRegistry().get(getCompiledTemplate().getLocation()) != null;

            if (!context.isInherited()) {
                Map<String, List<InheritanceBlock>> inheritanceChain = context.getInheritanceChain();
                Map<String, InheritanceBlock> inheritanceRegistry = getCompiledTemplate()
                        .getInheritanceRegistry();
                LinkedList<InheritanceBlock> chain;
                for (String key : inheritanceRegistry.keySet()) {
                    chain = new LinkedList<InheritanceBlock>();
                    chain.add(inheritanceRegistry.get(key));
                    inheritanceChain.put(key, chain);
                }
            }
            if (!include) {
                context.getMacroRegistry().putAll(getCompiledTemplate().getMacroRegistry());
            }
            if (!context.isInherited() && !include) {
                result.startDocument();
            }

            // update init context
            context.getInitList().addAll(0, getCompiledTemplate().getExtendList());
            
            return super.executeAndProceed(context, result);
        }

    }

    class EndStep extends Step {

        public EndStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            Map<String, IncludeBlock> includeRegistry = context.getIncludeRegistry();
            String key = getCompiledTemplate().getLocation();
            IncludeBlock inc = includeRegistry.get(key);
            if (inc != null) {
                // import or include
                Step next = inc.getEndStep();
                // remove mapping because of recursion detection
                includeRegistry.remove(key);

                return next;
            }

            if (!context.isInherited()) {
                result.endDocument();
            }
            return super.executeAndProceed(context, result);
        }

    }

}
