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
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

/**
 * Implementation of a "callMacro" template instruction.
 */
public class CallMacroBlock extends MacroBlock {

    protected static Log log = LogFactory.getLog(CallMacroBlock.class);

    public CallMacroBlock(SaxElement saxElement) {
        super(saxElement);
    }

    @Override
    protected Step buildStartStep(Locator locator, NamespacesHandle namespacesHandle) {
        return new StartStep(locator);
    }

    @Override
    protected Step buildEndStep(Locator locator) {
        return new EndStep(locator);
    }

    class StartStep extends Step {

        // compiledNext: first following instruction
        // runtimeNext idem

        public StartStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            context.getTemplateContext().saveContext();
            return super.executeAndProceed(context, result);
        }

    }

    class EndStep extends Step {

        // compiledNext: first following instruction after block
        // runtimeNext is the startstep.next of the called macro

        public EndStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            MacroBlock block = context.getMacroRegistry().get(getName());
            if (block == null) {
                throw new TemplateException("Macro " + getName() + " not found, location " + getLocation());
            }

            // Execute the macro
            context.getExecutor().execute(block.getStartStep().getCompiledNext(), block.getEndStep(), context, result);

            // Restore the context
            context.getTemplateContext().restoreContext();

            return getCompiledNext();
        }

    }

}
