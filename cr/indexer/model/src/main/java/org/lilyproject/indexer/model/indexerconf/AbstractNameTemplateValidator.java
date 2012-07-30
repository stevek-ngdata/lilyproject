package org.lilyproject.indexer.model.indexerconf;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.lilyproject.repository.api.QName;

public abstract class AbstractNameTemplateValidator implements NameTemplateValidator {

    private Set<Class> supportedTypes = new HashSet<Class>();
    private Set<String> variables = new HashSet<String>();
    private Set<String> booleanVariables = new HashSet<String>();
    private Set<String> variantProperties = new HashSet<String>();
    private Set<QName> fieldNames = new HashSet<QName>();

    protected AbstractNameTemplateValidator(Set<Class> supportedTypes, Set<String> variables,
                                            Set<String> booleanVariables, Set<String> variantProperties,
                                            Set<QName> fieldNames) {
        this.supportedTypes = supportedTypes;
        this.variables = variables;
        this.booleanVariables = booleanVariables;
        this.variantProperties = variantProperties;
        this.fieldNames = fieldNames;
    }

    @Override
    public void validate(NameTemplate template) throws NameTemplateException {
        for (TemplatePart part : template.getParts()) {
            if (!supportedTypes.contains(part.getClass())) {
                throw new NameTemplateException("Unsupported template part: " + part.getClass(),
                        template.getTemplate());
            }

            PartValidator validator = partValidators.get(part.getClass());
            if (validator == null) {
                throw new NameTemplateException("Don't know how to validate " + part.getClass(),
                        template.getTemplate());
            }

            validator.validate(template.getTemplate(), part);
        }
    }

    private interface PartValidator {
        void validate(String template, TemplatePart part) throws NameTemplateException;
    }

    private Map<Class, PartValidator> partValidators = new HashMap<Class, PartValidator>();

    {
        partValidators.put(LiteralTemplatePart.class, allOk());
        partValidators.put(ConditionalTemplatePart.class, conditionalValidator());
        partValidators.put(VariableTemplatePart.class, variableValidator());
        partValidators.put(FieldTemplatePart.class, fieldValidator());
        partValidators.put(VariantPropertyTemplatePart.class, variantPropertyValidator());
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
            public void validate(String template, TemplatePart part) throws NameTemplateException {
                String condition = ((ConditionalTemplatePart) part).getConditional();
                if (booleanVariables != null && !booleanVariables.contains(condition)) {
                    throw new NameTemplateException("No such boolean variable: " + condition, template);
                }
            }
        };
    }

    private PartValidator variableValidator() {
        return new PartValidator() {
            @Override
            public void validate(String template, TemplatePart part) throws NameTemplateException {
                String var = ((VariableTemplatePart) part).getVariable();
                if (variables != null && !variables.contains(var)) {
                    throw new NameTemplateException("No such variable: " + var, template);
                }
            }
        };
    }

    private PartValidator fieldValidator() {
        return new PartValidator() {
            @Override
            public void validate(String template, TemplatePart part) throws NameTemplateException {
                QName field = ((FieldTemplatePart) part).getFieldName();
                if (fieldNames != null && !fieldNames.contains(field)) {
                    throw new NameTemplateException("No such field: " + field, template);
                }
            }
        };
    }

    private PartValidator variantPropertyValidator() {
        return new PartValidator() {
            @Override
            public void validate(String template, TemplatePart part) throws NameTemplateException {
                String variantProperty = ((VariantPropertyTemplatePart) part).getName();
                 if (variantProperties != null && !variantProperties.contains(variantProperty)) {
                    throw new NameTemplateException("No such variant property: " + variantProperty, template);
                }
            }
        };
    }

}
