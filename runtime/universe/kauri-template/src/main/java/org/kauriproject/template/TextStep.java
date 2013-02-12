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

import org.kauriproject.template.el.CompositeExpression;
import org.kauriproject.template.el.ELFacade;
import org.kauriproject.template.el.Expression;
import org.kauriproject.xml.sax.Saxizer;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

public class TextStep extends Step {

    // EL
    private final CompositeExpression elExpr;

    public TextStep(ELFacade elFacade, Locator locator, char[] ch, int start, int length) {
        super(locator);
        String expression = String.valueOf(ch).substring(start, start + length);
        elExpr = elFacade.createExpression(expression, Object.class);
    }

    @Override
    public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
        // override it to generate a TemplateEvent

        if (!context.isSilencing()) {
            for (Expression expr: elExpr.getExpressions()) {
                Object value = expr.evaluate(context.getTemplateContext());
                Saxizer.toSax(value, result);
            }
        }

        return super.executeAndProceed(context, result);
    }

}
