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
import org.xml.sax.SAXException;

/**
 * Implementation of a "macro" template instruction.
 */
public class MacroBlock extends TemplateBlock {

    // MACRO CONSTANTS
    public static final String NAME = "name";

    // macro
    private String name;

    public MacroBlock(SaxElement saxElement) {
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

        // compiledNext: first nested instruction
        // runtimeNext endstep.next

        public StartStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            return getEndStep().getCompiledNext().executeAndProceed(context, result);
        }
    }

    class EndStep extends Step {

        // compiledNext: first following instruction after block
        // runtimeNext endstep.next of the 'calling' macro

        public EndStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            throw new TemplateException("This situation should never occur.");
        }

    }

}
