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

import java.util.List;

import org.xml.sax.Locator;
import org.xml.sax.SAXException;

/**
 * Implementation of a "block" (template inheritance) instruction.
 */
public class InheritanceBlock extends TemplateBlock {

    // BLOCK CONSTANTS
    public static final String NAME = "name";

    // block
    private String name;

    public InheritanceBlock(SaxElement saxElement) {
        super(saxElement);
        this.name = saxElement.getAttributes().getValue(NAME);
    }

    protected Step buildStartStep(Locator locator, NamespacesHandle namespacesHandle) {
        return new StartStep(locator);
    }

    protected Step buildEndStep(Locator locator) {
        return new EndStep(locator);
    }

    public String getName() {
        return name;
    }

    class StartStep extends Step {

        public StartStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            context.getTemplateContext().saveContext();
            InheritanceBlock block = context.getInheritanceChain().get(getName()).get(0);
            Step step = block.getStartStep().getCompiledNext();
            return step.executeAndProceed(context, result);
        }
    }

    class EndStep extends Step {

        public EndStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            context.getTemplateContext().restoreContext();
            
            // lookup base block and execute baseblock.endstep.next
            if (context.isInherited()) {
                // check if this block is executed as a superblock and return to the calling block
                if (!context.getSuperBlockStack().isEmpty()) {
                    SuperBlock superBlock = context.getSuperBlockStack().peek();
                    return superBlock.getEndStep();
                }
                // otherwise: lookup highest overridden block in chain
                List<InheritanceBlock> chain = context.getInheritanceChain().get(getName());
                if (chain != null && chain.size() > 0) {
                    return chain.get(chain.size() - 1).getEndStep().getCompiledNext();
                } else {
                    throw new TemplateException("Parent block not found for " + getName() + ", location "
                            + getLocation());
                }
            }
            // this is the only possible block
            return getCompiledNext();
        }
    }

}
