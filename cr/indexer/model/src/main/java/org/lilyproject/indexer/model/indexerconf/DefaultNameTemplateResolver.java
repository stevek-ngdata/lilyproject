package org.lilyproject.indexer.model.indexerconf;

import java.util.HashMap;
import java.util.Map;

public class DefaultNameTemplateResolver implements NameTemplateResolver {

    private Map<String, Object> values = new HashMap<String, Object>();

    public DefaultNameTemplateResolver(Map<String, Object> values) {
        this.values = values;
    }

    @Override
    public Object resolve(TemplatePart part) {
        if (part instanceof ConditionalTemplatePart) {
            ConditionalTemplatePart cPart = (ConditionalTemplatePart)part;

            //FIXME: throw NTVE if conditional doesn't point to a Boolean
            if ((Boolean)values.get(cPart.getConditional())) {
                return cPart.getTrueString();
            } else {
                return cPart.getFalseString();
            }
        } else if (part instanceof VariableTemplatePart) {
            VariableTemplatePart varPart = (VariableTemplatePart) part;
            return values.get(varPart.getVariable());
        } else if (part instanceof LiteralTemplatePart) {
            return ((LiteralTemplatePart)part).getString();
        } else {
            throw new NameTemplateEvaluationException("Unsupported TemplatePart class: " + part.getClass().getName());
        }

    }

}
