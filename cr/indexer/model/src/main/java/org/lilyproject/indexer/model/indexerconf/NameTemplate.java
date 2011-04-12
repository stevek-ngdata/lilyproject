package org.lilyproject.indexer.model.indexerconf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NameTemplate {
    private String originalTemplate;
    private List<TemplatePart> parts = new ArrayList<TemplatePart>();
    private static Pattern varPattern = Pattern.compile("\\$\\{([^\\}]+)\\}");
    private static Pattern exprPattern = Pattern.compile("([^\\?]+)\\?([^:]+)(?::(.+))?");

    public NameTemplate(String template) throws NameTemplateException {
        this(template, null, null);
    }

    /**
     *
     * @param variables the names of the available variables. Allows up-front validation of the expression.
     * @param booleanVariables the names of the available boolean variables.
     */
    public NameTemplate(String template, Set<String> variables, Set<String> booleanVariables)
            throws NameTemplateException {
        this.originalTemplate = template;
        parse(template, variables, booleanVariables);
    }

    private void parse(String template, Set<String> variables, Set<String> booleanVariables)
            throws NameTemplateException {
        int pos = 0;
        Matcher matcher = varPattern.matcher(template);
        while (pos < template.length()) {
            if (matcher.find(pos)) {
                int start = matcher.start();
                if (start > pos) {
                    parts.add(new LiteralTemplatePart(template.substring(pos, start)));
                }
                String expr = matcher.group(1);
                Matcher exprMatcher = exprPattern.matcher(expr);
                if (exprMatcher.matches()) {
                    String condition = exprMatcher.group(1);
                    String trueValue = exprMatcher.group(2);
                    String falseValue = exprMatcher.group(3) != null ? exprMatcher.group(3) : "";
                    if (booleanVariables != null && !booleanVariables.contains(condition)) {
                        throw new NameTemplateException("No such boolean variable: " + condition, template);
                    }
                    parts.add(new ConditionalTemplatePart(condition, trueValue, falseValue));
                } else {
                    if (variables != null && !variables.contains(expr)) {
                        throw new NameTemplateException("No such variable: " + expr, template);
                    }
                    parts.add(new VariableTemplatePart(expr));
                }

                pos = matcher.end();
            } else {
                parts.add(new LiteralTemplatePart(template.substring(pos)));
                break;
            }
        }
    }

    public String format(Map<String, Object> context) throws NameTemplateEvaluationException {
        StringBuilder result = new StringBuilder();
        for (TemplatePart part : parts) {
            part.eval(result, context);
        }
        return result.toString();
    }

    private static interface TemplatePart {
        void eval(StringBuilder builder, Map<String,  Object> context) throws NameTemplateEvaluationException;
    }

    private class LiteralTemplatePart implements TemplatePart {
        private String string;

        public LiteralTemplatePart(String string) {
            this.string = string;
        }

        @Override
        public void eval(StringBuilder builder, Map<String, Object> context) {
            builder.append(string);
        }
    }

    private class VariableTemplatePart implements TemplatePart {
        private String variableName;

        public VariableTemplatePart(String variableName) {
            this.variableName = variableName;
        }

        @Override
        public void eval(StringBuilder builder, Map<String, Object> context) throws NameTemplateEvaluationException {
            Object value = context.get(variableName);
            if (value == null) {
                throw new NameTemplateEvaluationException("Variable does not evaluate to a value: " + variableName,
                        originalTemplate);
            }
            builder.append(value);
        }
    }

    private class ConditionalTemplatePart implements TemplatePart {
        private String conditional;
        private String trueString;
        private String falseString;

        public ConditionalTemplatePart(String conditional, String trueString, String falseString) {
            this.conditional = conditional;
            this.trueString = trueString;
            this.falseString = falseString;
        }

        @Override
        public void eval(StringBuilder builder, Map<String, Object> context) throws NameTemplateEvaluationException {
            Object condVal = context.get(conditional);
            if (condVal == null) {
                throw new NameTemplateEvaluationException("Variable does not evaluate to a value: " + conditional,
                        originalTemplate);
            }

            if (!(condVal instanceof Boolean)) {
                throw new NameTemplateEvaluationException("Variable is not a boolean: " + conditional, originalTemplate);
            }

            builder.append(condVal.equals(Boolean.TRUE) ? trueString : falseString);
        }
    }
}
