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
package org.kauriproject.template.el;

import javax.el.ELContext;
import javax.el.ExpressionFactory;
import javax.el.ValueExpression;

import org.kauriproject.template.TemplateContext;

/**
 * Implementation of a EL expression.
 */
public class ELExpression implements Expression {

    private ValueExpression valex;

    public ELExpression(ValueExpression valex) {
        this.valex = valex;
    }

    public ELExpression(String elString, Class<?> expectedType, ExpressionFactory factory,
            ParseContext parseContext) {
        this.valex = factory.createValueExpression(parseContext, elString, expectedType);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.kauriproject.template.el.Expression#evaluate(org.kauriproject.template.TemplateContext)
     */
    public Object evaluate(TemplateContext templateContext) {
        ELContext evalContext = new EvaluationContext(templateContext);
        return valex.getValue(evalContext);
    }

}
