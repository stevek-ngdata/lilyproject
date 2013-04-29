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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.util.repo.SystemFields;
import org.w3c.dom.Element;

public class NameTemplateParser {

    private static Pattern varPattern = Pattern.compile("\\$\\{([^\\}]+)\\}");
    private static Pattern exprPattern = Pattern.compile("([^\\?]+)\\?([^:]+)(?::(.+))?");
    private static Pattern variantPropertyPattern = Pattern.compile("vprop:([^:]+)");
    private static Pattern fieldPattern = Pattern.compile("([^:]+):([^:]+)?");

    // used for parsing qnames
    private Repository repository;
    private SystemFields systemFields;

    public NameTemplateParser() {
        this(null, null);
    }

    public NameTemplateParser(Repository repository, SystemFields systemFields) {
        this.repository = repository;
        this.systemFields = systemFields;
    }

    public NameTemplate parse(String template, NameTemplateValidator validator)
            throws IndexerConfException, NameTemplateException, InterruptedException, RepositoryException {
        return parse(null, template, validator);
    }

    // FIXME: Not very clean that this can throw IndexerConfException
    public NameTemplate parse(Element el, String template, NameTemplateValidator validator)
            throws IndexerConfException, NameTemplateException, InterruptedException, RepositoryException {
        List<TemplatePart> parts = new ArrayList<TemplatePart>();
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
                Matcher atVariantPropMatcher = variantPropertyPattern.matcher(expr);
                Matcher fieldMatcher = fieldPattern.matcher(expr);
                if (exprMatcher.matches()) {
                    String condition = exprMatcher.group(1);
                    String trueValue = exprMatcher.group(2);
                    String falseValue = exprMatcher.group(3) != null ? exprMatcher.group(3) : "";

                    parts.add(new ConditionalTemplatePart(condition, trueValue, falseValue));
                } else if (atVariantPropMatcher.matches()) {
                    parts.add(new VariantPropertyTemplatePart(atVariantPropMatcher.group(1)));
                } else if (fieldMatcher.matches()) {
                    parts.add(buildFieldTemplatePart(el, template, expr));
                } else {
                    parts.add(new VariableTemplatePart(expr));
                }

                pos = matcher.end();
            } else {
                parts.add(new LiteralTemplatePart(template.substring(pos)));
                break;
            }
        }

        final NameTemplate nameTemplate = new NameTemplate(template, parts);
        if (validator != null) {
            validator.validate(nameTemplate);
        }
        return nameTemplate;
    }

    private TemplatePart buildFieldTemplatePart(Element el, String template, String expr)
            throws IndexerConfException, InterruptedException, RepositoryException {
        QName field = ConfUtil.parseQName(expr, el);
        FieldType fieldType = ConfUtil.getFieldType(field, systemFields, repository.getTypeManager());
        return new FieldTemplatePart(fieldType, field);
    }

}
