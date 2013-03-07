/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.indexer.model.indexerconf;

import java.util.HashMap;
import java.util.Map;

public class DynamicFieldNameTemplateResolver implements NameTemplateResolver {

    private Map<String, Object> values = new HashMap<String, Object>();

    public DynamicFieldNameTemplateResolver(Map<String, Object> values) {
        this.values = values;
    }

    @Override
    public Object resolve(TemplatePart part) {
        if (part instanceof ConditionalTemplatePart) {
            ConditionalTemplatePart cPart = (ConditionalTemplatePart)part;

            Object condVal = values.get(cPart.getConditional());
            if (condVal == null) {
                throw new NameTemplateEvaluationException("Variable does not evaluate to a value: " + cPart.getConditional());
            }

            if (!(condVal instanceof Boolean)) {
                throw new NameTemplateEvaluationException("Variable is not a boolean: " + cPart.getConditional());
            }


            if ((Boolean)condVal) {
                return cPart.getTrueString();
            } else {
                return cPart.getFalseString();
            }
        } else if (part instanceof VariableTemplatePart) {
            VariableTemplatePart varPart = (VariableTemplatePart) part;
            Object value = values.get(varPart.getVariable());
            if (value == null) {
                throw new NameTemplateEvaluationException("Variable does not evaluate to a value: " + varPart.getVariable());

            }
            return value;
        } else if (part instanceof LiteralTemplatePart) {
            return ((LiteralTemplatePart)part).getString();
        } else {
            throw new NameTemplateEvaluationException("Unsupported TemplatePart class: " + part.getClass().getName());
        }

    }

}
