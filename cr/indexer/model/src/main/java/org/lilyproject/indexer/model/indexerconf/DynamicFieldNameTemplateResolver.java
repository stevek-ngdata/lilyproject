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
