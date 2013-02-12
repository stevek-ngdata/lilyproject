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

import java.util.Map;

import org.kauriproject.template.el.ELFacade;
import org.kauriproject.template.el.Expression;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.Attributes;

/**
 * Implementation of an "import" template instruction.
 */
public class ImportBlock extends IncludeBlock {

    public ImportBlock(ELFacade elFacade, SaxElement saxElement, TemplateService templateService) {
        super(elFacade, saxElement, templateService);
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

        private final Expression sourceExpr;

        public StartStep(Locator locator) {
            super(locator);

            Attributes attributes = getSaxElement().getAttributes();

            String srcAttr = attributes.getValue(SRC);
            if (srcAttr == null) {
                throw new TemplateException(SRC + " attribute is required on " + Directive.IMPORT.getTagName());
            }
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
            CompiledTemplate imported;
            try {
                // ask service to get compiled template for sourcelocation
                // this may trigger a build of the template-to-include
                imported = templateService.buildTemplate(sourceLocation, context, context.isSilencing());
            } catch (Exception ex) {
                throw new TemplateException("Error parsing template included at location " + getLocation(), ex);
            }

            IncludeBlock old = context.getIncludeRegistry().put(imported.getLocation(), ImportBlock.this);
            // detect recursion
            if (old != null) {
                throw new TemplateException("KTL: recursion is not allowed in import, location "
                        + getLocation());
            }

            // merge macro mappings - append mappings, no overwrites !
            Map<String, MacroBlock> macroRegistry = context.getMacroRegistry();
            Map<String, MacroBlock> importRegistry = imported.getMacroRegistry();
            for (String macro : importRegistry.keySet()) {
                if (!macroRegistry.containsKey(macro)) {
                    macroRegistry.put(macro, importRegistry.get(macro));
                }
            }

            // start new template and init context
            context.getTemplateContext().saveContext();
            context.saveInit();

            return imported;
        }

    }

    class EndStep extends Step {

        public EndStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            if (Mode.SILENT == includeMode) {
                context.setSilencing(false);
            }

            // restore template and init context
            context.getTemplateContext().restoreContext();
            context.restoreInit();
            return super.executeAndProceed(context, result);
        }
    }
}
