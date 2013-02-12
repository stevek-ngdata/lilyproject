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

import org.kauriproject.template.el.Expression;
import org.xml.sax.Locator;

public abstract class TemplateBlock {

    private SaxElement saxElement;
    protected Step startStep;
    protected Step endStep;

    public TemplateBlock() {
        this( null);
    }

    public TemplateBlock(SaxElement saxElement) {
        this.saxElement = saxElement;
    }

    public Step getStartStep() {
        return startStep;
    }

    public Step getEndStep() {
        return endStep;
    }

    public Step createStartStep(Locator locator, NamespacesHandle namespacesHandle) {
        this.startStep = buildStartStep(locator, namespacesHandle);
        return startStep;
    }

    public Step createEndStep(Locator locator) {
        this.endStep = buildEndStep(locator);
        return endStep;
    }

    protected abstract Step buildStartStep(Locator locator, NamespacesHandle namespacesHandle);

    protected abstract Step buildEndStep(Locator locator);

    public SaxElement getSaxElement() {
        return saxElement;
    }
    
    public String stringEval(final ExecutionContext context, final Expression expr, final String dfault) {
        return expr != null ? (String)expr.evaluate(context.getTemplateContext()): dfault;
    }
}
