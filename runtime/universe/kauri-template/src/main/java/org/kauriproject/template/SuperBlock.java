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
 * Implementation of a "superBlock" template instruction (nested in a "block" instruction).
 */
public class SuperBlock extends TemplateBlock {

    /**
     * ref to parent "block" block
     */
    protected InheritanceBlock inheritanceBlock;

    public SuperBlock(SaxElement saxElement, InheritanceBlock inheritanceBlock) {
        super(saxElement);
        this.inheritanceBlock = inheritanceBlock;
    }

    protected Step buildStartStep(Locator locator, NamespacesHandle namespacesHandle) {
        return new StartStep(locator);
    }

    protected Step buildEndStep(Locator locator) {
        return new EndStep(locator);
    }

    class StartStep extends Step {

        public StartStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            if (!context.isInherited()) {
                throw new TemplateException(
                        "No template inheritance, so call to super block not possible, location "
                                + getLocation());
            }

            context.getTemplateContext().saveContext();
            context.getSuperBlockStack().push(SuperBlock.this);
            // lookup parent block
            List<InheritanceBlock> chain = context.getInheritanceChain().get(inheritanceBlock.getName());
            if (chain != null && chain.size() > 0) {
                int index = chain.indexOf(inheritanceBlock);
                return chain.get(index + 1).getStartStep().getCompiledNext();
            } else {
                throw new TemplateException("Parent block not found for " + inheritanceBlock.getName()
                        + ", location " + getLocation());
            }

        }

    }

    class EndStep extends Step {

        public EndStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            context.getSuperBlockStack().pop();
            return super.executeAndProceed(context, result);
        }
    }

}
