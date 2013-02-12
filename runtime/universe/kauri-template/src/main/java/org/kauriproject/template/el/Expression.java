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

import org.kauriproject.template.TemplateContext;

/**
 * Blueprint for Kauri EL expressions.
 */
public interface Expression {

    /**
     * Boundary for the length of the id that identifies the EL parser.
     */
    public static final int ID_MAX_LENGTH = 3;

    /**
     * Supported EL parsers.
     */
    public enum ExpressionParser {
        /**
         * EL
         */
        EL(""),
        /**
         * Groovy
         */
        GROOVY("g");

        private final String id;

        ExpressionParser(String id) {
            if (id.length() > ID_MAX_LENGTH) {
                this.id = id.substring(0, ID_MAX_LENGTH);
            } else {
                this.id = id;
            }
        }

        /**
         * Returns null if value is not recongized.
         */
        public static ExpressionParser fromString(String value) {
            if (value.equals(EL.id))
                return EL;
            else if (value.equals(GROOVY.id))
                return GROOVY;
            else
                return null;
        }
    }

    /**
     * Evaluate the expression against the given template context.
     */
    public Object evaluate(TemplateContext templateContext);

}
