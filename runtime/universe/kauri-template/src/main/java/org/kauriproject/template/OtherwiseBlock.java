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

/**
 * Implementation of a "otherwise" template instruction (nested in a "choose" instruction).
 */
public class OtherwiseBlock extends TemplateBlock {

    protected static Log log = LogFactory.getLog(OtherwiseBlock.class);


    /**
     * ref to parent "choose" block
     */
    protected ChooseBlock chooseBlock;

    public OtherwiseBlock(SaxElement saxElement, ChooseBlock chooseBlock) {
        super(saxElement);
        this.chooseBlock = chooseBlock;
    }

    protected Step buildStartStep(Locator locator, NamespacesHandle namespacesHandle) {
        return new StartStep(locator);
    }

    protected Step buildEndStep(Locator locator) {
        return new EndStep(locator);
    }

    class StartStep extends Step {

        // compiledNext: first nested instruction
        // runtimeNext idem

        public StartStep(Locator locator) {
            super(locator);
        }
    }

    class EndStep extends Step {

        // compiledNext: first following instruction after block, i.e. chooseBlock.endStep
        // runtimeNext: we prefer to skip a step and goto chooseBlock.endStep.compiledNext

        public EndStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) {
            return chooseBlock.getEndStep().getCompiledNext();
        }

    }

}
