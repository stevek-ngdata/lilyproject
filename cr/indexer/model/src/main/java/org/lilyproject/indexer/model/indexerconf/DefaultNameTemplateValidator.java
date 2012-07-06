package org.lilyproject.indexer.model.indexerconf;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DefaultNameTemplateValidator implements NameTemplateValidator {

    private Set<Class> supportedTypes = new HashSet<Class>();
    private Set<String> variables = new HashSet<String>();
    private Set<String> booleanVariables = new HashSet<String>();

    public DefaultNameTemplateValidator(Set<Class> supportedTypes, Set<String> variables, Set<String> booleanVariables) {
        this.supportedTypes = supportedTypes;
        this.variables = variables;
        this.booleanVariables = booleanVariables;
    }

    @Override
    public void validate(NameTemplate template) throws NameTemplateException {
        for (TemplatePart part: template.getParts()) {
            if (!supportedTypes.contains(part.getClass())) {
                throw new NameTemplateException("Unsupported template part: " + part.getClass(), template.getTemplate());
            }

            PartValidator validator = partValidators.get(part.getClass());
            if (validator == null) {
                throw new NameTemplateException("Don't know how to validate " + part.getClass(), template.getTemplate());
            }

            validator.validate(template.getTemplate(), part);
        }
    }

    private interface PartValidator {
        void validate(String template, TemplatePart part);
    }

    private Map<Class, PartValidator> partValidators = new HashMap<Class, PartValidator>();

    {
        partValidators.put(LiteralTemplatePart.class, allOk());
        partValidators.put(ConditionalTemplatePart.class, conditionalValidator());
        partValidators.put(VariableTemplatePart.class, variableValidator());
        partValidators.put(FieldTemplatePart.class, fieldValidator());
        partValidators.put(VariantPropertyTemplatePart.class, allOk());
    }

    private PartValidator allOk() {
        return new PartValidator() {
            @Override
            public void validate(String template, TemplatePart part) {
                // ok!
            }
        };
    }

    private PartValidator conditionalValidator() {
        return new PartValidator() {
            @Override
            public void validate(String template, TemplatePart part) {
                String condition = ((ConditionalTemplatePart)part).getConditional();
                if (booleanVariables != null && !booleanVariables.contains(condition)) {
                    //TODO: exception!
                }
            }
        };
    }

    private PartValidator variableValidator() {
        return new PartValidator() {
            @Override
            public void validate(String template, TemplatePart part) {
                String var = ((VariableTemplatePart)part).getVariable();
                if (variables != null && !variables.contains(var)) {
                    //TODO: exception
                }
            }
        };
    }

    private PartValidator fieldValidator() {
        return new PartValidator() {
            @Override
            public void validate(String template, TemplatePart part) {
                // OK: field name is validated during parsing
            }
        };
    }

}
