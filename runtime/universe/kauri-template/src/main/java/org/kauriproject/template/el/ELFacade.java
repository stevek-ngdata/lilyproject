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

import javax.el.ExpressionFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kauriproject.template.el.Expression.ExpressionParser;
import org.jboss.el.ExpressionFactoryImpl;

/**
 * Facade to shield implementation specific details of the expression language(s).
 */
public class ELFacade {

    private static Log log = LogFactory.getLog(ELFacade.class);

    private ExpressionFactory factory;
    private ParseContext parseContext;

    public ELFacade() {
        this(null);
    }

    public ELFacade(FunctionRegistry functionRegistry) {
        factory = new ExpressionFactoryImpl();
        // The below is for Juel
        // TreeBuilder builder = new Builder(Builder.Feature.METHOD_INVOCATIONS, Builder.Feature.VARARGS);
        // factory = new ExpressionFactoryImpl(new TreeStore(builder, new Cache(10)));
        parseContext = new ParseContext(functionRegistry);
    }

    public CompositeExpression createExpression(String expression, Class<?> expectedType) {
        if (expression == null ) 
            return null;
        CompositeExpression cex = new CompositeExpression();
        parseExpression(expression, expectedType, cex);
        return cex;
    }

    private void parseExpression(final String expression, final Class<?> expectedType,
            final CompositeExpression cex) {
        int start = getStartPosition(expression, 0);
        int brace = expression.indexOf('{', start);
        if (start < 0 || brace < 0) {
            // not a valid expression: parse as literal
            cex.add(new LiteralExpression(sanitize(expression), expectedType));
        } else {
            String id;
            if (start > 0) {
                cex.add(new LiteralExpression(sanitize(expression.substring(0, start)), expectedType));
            }
            id = expression.substring(start + 1, brace);
            int end = getEndPosition(expression, brace + 1);
            if (id.length() > Expression.ID_MAX_LENGTH || id.contains(" ") || end < 0) {
                // not a valid expression, parse start as literal
                cex.add(new LiteralExpression(expression.substring(start, start + 1), expectedType));
                // try to parse rest of the expression
                parseExpression(expression.substring(start + 1), expectedType, cex);
            } else {
                ExpressionParser parser = ExpressionParser.fromString(id);
                String exp;
                if (parser == ExpressionParser.EL) {
                    exp = expression.substring(start, end + 1);
                    cex.add(new ELExpression(exp, expectedType, factory, parseContext));
                } else if (parser == ExpressionParser.GROOVY) {
                    exp = expression.substring(brace + 1, end);
                    cex.add(new GroovyExpression(exp));
                } else {
                    log.warn("expression type '" + id + "' not supported");
                    cex.add(new LiteralExpression(sanitize(expression.substring(start, end + 1)),
                            expectedType));
                }

                if (++end < expression.length()) {
                    parseExpression(expression.substring(end), expectedType, cex);
                }
            }
        }
    }

    /**
     * Get start position of EL expression.
     */
    private int getStartPosition(final String expression, final int from) {
        int start1 = getStartPosition(expression, from, '$');
        int start2 = getStartPosition(expression, from, '#');
        if (start2 < 0)
            return start1;
        else if (start1 < 0)
            return start2;
        else
            return Math.min(start1, start2);
    }

    private int getStartPosition(final String expression, final int from, final char kar) {
        int start = expression.indexOf(kar, from);
        // check escaped
        if (start > 0 && expression.charAt(start - 1) == '\\') {
            return getStartPosition(expression, start + 1, kar);
        } else {
            return start;
        }
    }

    private int getEndPosition(final String expression, final int from) {
        int start = expression.indexOf('{', from);
        int end = expression.indexOf('}', from);
        if (start < 0 || end < start) {
            return end;
        } else {
            int inter = getEndPosition(expression, ++start);
            return getEndPosition(expression, ++inter);
        }
    }

    /**
     * Remove escape characters.
     */
    private String sanitize(String literal) {
        return literal.replaceAll("\\\\([$#])", "$1");
    }

}
