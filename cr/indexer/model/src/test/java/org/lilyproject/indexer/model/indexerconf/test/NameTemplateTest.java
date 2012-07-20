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
package org.lilyproject.indexer.model.indexerconf.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Test;
import org.lilyproject.indexer.model.indexerconf.ConditionalTemplatePart;
import org.lilyproject.indexer.model.indexerconf.DefaultNameTemplateValidator;
import org.lilyproject.indexer.model.indexerconf.DynamicFieldNameTemplateResolver;
import org.lilyproject.indexer.model.indexerconf.LiteralTemplatePart;
import org.lilyproject.indexer.model.indexerconf.NameTemplate;
import org.lilyproject.indexer.model.indexerconf.NameTemplateEvaluationException;
import org.lilyproject.indexer.model.indexerconf.NameTemplateException;
import org.lilyproject.indexer.model.indexerconf.NameTemplateParser;
import org.lilyproject.indexer.model.indexerconf.NameTemplateResolver;
import org.lilyproject.indexer.model.indexerconf.NameTemplateValidator;
import org.lilyproject.indexer.model.indexerconf.VariableTemplatePart;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * Broken during refactoring
 * Should be able to fix this if we move helper methods like IndexerConfBuilder.parseQName() to a shared component
 */
public class NameTemplateTest {

    @Test
    public void testLiteral() throws Exception {
        NameTemplate template = new NameTemplateParser().parse("foobar", defaultValidator());
        assertEquals("foobar", template.format(getResolver()));
    }

    @Test
    public void testVar() throws Exception {
        NameTemplate template = new NameTemplateParser().parse("${var1}", defaultValidator());
        assertEquals("hello", template.format(getResolver()));
    }

    @Test
    public void testEmbeddedVar() throws Exception {
        NameTemplate template = new NameTemplateParser().parse("prefix_${var1}_suffix", defaultValidator());
        assertEquals("prefix_hello_suffix", template.format(getResolver()));
    }

    @Test
    public void testCond() throws Exception {
        NameTemplate template = new NameTemplateParser().parse("${bool1?yes:no}", defaultValidator());
        assertEquals("yes", template.format(getResolver()));

        template = new NameTemplateParser().parse("${bool2?yes:no}", defaultValidator());
        assertEquals("no", template.format(getResolver()));

        template = new NameTemplateParser().parse("${bool2?yes}", defaultValidator());
        assertEquals("", template.format(getResolver()));

        template = new NameTemplateParser().parse("${bool1?yes}", defaultValidator());
        assertEquals("yes", template.format(getResolver()));

        // test with disabled validator because we want it to fail @ evaluation time
        template = new NameTemplateParser().parse("${var1?yes:no}", disabledValidator());
        try {
            assertEquals("no", template.format(getResolver()));
            fail("Expected exception");
        } catch (NameTemplateEvaluationException e) {
            // expected
        }

        // test with disabled validator because we want it to fail @ evaluation time
        template = new NameTemplateParser().parse("${nonexisting?yes:no}", disabledValidator());
        try {
            assertEquals("no", template.format(getResolver()));
            fail("Expected exception");
        } catch (NameTemplateEvaluationException e) {
            // expected
        }
    }

    @Test
    public void testCondEmbedded() throws Exception {
        NameTemplate template = new NameTemplateParser().parse("prefix_${bool1?yes:no}_suffix", defaultValidator());
        assertEquals("prefix_yes_suffix", template.format(getResolver()));
    }

    @Test
    public void testIncompleteExpr() throws Exception {
        NameTemplate template = new NameTemplateParser().parse("${", defaultValidator());
        assertEquals("${", template.format(getResolver()));

        template = new NameTemplateParser().parse("x${x${x", defaultValidator());
        assertEquals("x${x${x", template.format(getResolver()));

        template = new NameTemplateParser().parse("x${}", defaultValidator());
        assertEquals("x${}", template.format(getResolver()));

        // test with disabled validator because we want it to fail @ evaluation time
        template = new NameTemplateParser().parse("x${?}", disabledValidator());
        try {
            assertEquals("x${?}", template.format(getResolver()));
            fail("expected exception");
        } catch (NameTemplateEvaluationException e) {
            // expected
        }

        // test with disabled validator because we want it to fail @ evaluation time
        template = new NameTemplateParser().parse("x${a?}", disabledValidator());
        try {
            assertEquals("x${a?}", template.format(getResolver()));
            fail("expected exception");
        } catch (NameTemplateEvaluationException e) {
            // expected
        }
    }

    @Test(expected = NameTemplateException.class)
    public void testInvalidExpressionNoSuchBooleanVariable() throws Exception {
        new NameTemplateParser()
                .parse("${foo?true:false}",
                        new DefaultNameTemplateValidator(templatePartTypes(), null, Sets.newHashSet("list")));
    }

    @Test(expected = NameTemplateException.class)
    public void testInvalidExpressionNoSuchVariable() throws Exception {
        new NameTemplateParser()
                .parse("${variable}",
                        new DefaultNameTemplateValidator(templatePartTypes(), Sets.newHashSet("anothervariable"),
                                null));
    }

    private NameTemplateValidator defaultValidator() {
        return new DefaultNameTemplateValidator(templatePartTypes(), defaultContext().keySet(),
                defaultBooleanVariables());
    }

    private NameTemplateValidator disabledValidator() {
        return new DefaultNameTemplateValidator(templatePartTypes(), null, null);
    }

    private Set<String> defaultBooleanVariables() {
        return Sets.newHashSet("bool1", "bool2");
    }

    public NameTemplateResolver getResolver() {
        return new DynamicFieldNameTemplateResolver(defaultContext());
    }

    private Map<String, Object> defaultContext() {
        Map<String, Object> context = new HashMap<String, Object>();
        context.put("var1", "hello");
        context.put("bool1", Boolean.TRUE);
        context.put("bool2", Boolean.FALSE);
        return context;
    }

    private Set<Class> templatePartTypes() {
        Set<Class> result = Sets.newHashSet();
        result.add(LiteralTemplatePart.class);
        result.add(VariableTemplatePart.class);
        result.add(ConditionalTemplatePart.class);
        return result;
    }
}
