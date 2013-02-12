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

import java.util.ArrayList;
import java.util.List;

import org.kauriproject.template.TemplateContext;

/**
 * Implementation of a composite expression.
 */
public class CompositeExpression implements Expression {

    private List<Expression> expressions;

    public CompositeExpression() {
        expressions = new ArrayList<Expression>();
    }

    public void add(Expression expression) {
        expressions.add(expression);
    }

    public List<Expression> getExpressions() {
        return expressions;
    }

    public Object evaluate(TemplateContext templateContext) {
        if (expressions.size() == 1) {
            return expressions.get(0).evaluate(templateContext);
        } else {
            StringBuffer buffer = new StringBuffer();
            for (Expression exp : expressions) {
                buffer.append(exp.evaluate(templateContext));
            }
            return buffer.toString();
        }

    }

}
