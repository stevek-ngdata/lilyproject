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

import org.kauriproject.template.el.ELFacade;
import org.kauriproject.template.el.Expression;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

/**
 * Implementation of a "include" template instruction.
 */
public class IncludeBlock extends TemplateBlock {

    // INCLUDE CONSTANTS
    public static final String SRC = "src";
    public static final String MODE = "mode";

    protected ELFacade elFacade;
    protected TemplateService templateService;

    protected Mode includeMode = null;

    public IncludeBlock(ELFacade elFacade, SaxElement saxElement, TemplateService templateService) {
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

        private final Expression sourceExpr;

        public StartStep(Locator locator) {
            super(locator);

            Attributes attributes = getSaxElement().getAttributes();

            String srcAttr = attributes.getValue(SRC);
            if (srcAttr == null)
                throw new TemplateException(SRC + " attribute is required on " + Directive.INCLUDE.getTagName());

            sourceExpr = elFacade.createExpression(srcAttr, String.class);
            String mode = attributes.getValue(MODE);
            if (mode != null) {
                includeMode = Mode.fromString(mode);
            }
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            if (Mode.SILENT == includeMode) {
                context.setSilencing(true);
            }

            String sourceLocation = (String) sourceExpr.evaluate(context.getTemplateContext());
            sourceLocation = context.getSourceResolver().toAbsolutePath(sourceLocation, context.getBaseUri());
            CompiledTemplate included;
            try {
                // ask service to get compiled template for sourcelocation
                // this may trigger a build of the template-to-include
                included = templateService.buildTemplate(sourceLocation, context, context.isSilencing());
            } catch (Exception ex) {
                throw new TemplateException("Error parsing template included at location " + getLocation(), ex);
            }

            IncludeBlock old = context.getIncludeRegistry().put(included.getLocation(), IncludeBlock.this);
            // detect recursion
            if (old != null) {
                throw new TemplateException("Recursion is not allowed in include, location " + getLocation());
            }

            // merge macro mappings
            context.getMacroRegistry().putAll(included.getMacroRegistry());

            // start new init context
            context.saveInit();

            return included;
        }

    }

    class EndStep extends Step {

        // compiledNext: first following instruction after block
        // runtimeNext idem

        public EndStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            if (Mode.SILENT == includeMode) {
                context.setSilencing(false);
            }
            // restore init context
            context.restoreInit();
            return super.executeAndProceed(context, result);
        }

    }

    enum Mode {
        REGULAR, SILENT;

        public static Mode fromString(String mode) {
            if (mode.equals("regular")) {
                return REGULAR;
            } else if (mode.equals("silent")) {
                return SILENT;
            } else {
                return null;
            }
        }
    }

}
