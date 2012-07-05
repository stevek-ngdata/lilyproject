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

import org.junit.Test;
import org.lilyproject.indexer.model.indexerconf.DefaultNameTemplateResolver;
import org.lilyproject.indexer.model.indexerconf.NameTemplate;
import org.lilyproject.indexer.model.indexerconf.NameTemplateEvaluationException;
import org.lilyproject.indexer.model.indexerconf.NameTemplateParser;
import org.lilyproject.indexer.model.indexerconf.NameTemplateResolver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * Broken during refactoring
 * Should be able to fix this if we move helper methods like IndexerConfBuilder.parseQName() to a shared component
 */
public class NameTemplateTest {

    @Test
    public void testLiteral() throws Exception {
        NameTemplate template = new NameTemplateParser().parse("foobar");
        assertEquals("foobar", template.format(null));
    }

    @Test
    public void testVar() throws Exception {
        NameTemplate template = new NameTemplateParser().parse("${var1}");
        assertEquals("hello", template.format(getResolver()));
    }

    @Test
    public void testEmbeddedVar() throws Exception {
        NameTemplate template = new NameTemplateParser().parse("prefix_${var1}_suffix");
        assertEquals("prefix_hello_suffix", template.format(getResolver()));
    }

    @Test
    public void testCond() throws Exception {
        NameTemplate template = new NameTemplateParser().parse("${bool1?yes:no}");
        assertEquals("yes", template.format(getResolver()));

        template = new NameTemplateParser().parse("${bool2?yes:no}");
        assertEquals("no", template.format(getResolver()));

        template = new NameTemplateParser().parse("${bool2?yes}");
        assertEquals("", template.format(getResolver()));

        template = new NameTemplateParser().parse("${bool1?yes}");
        assertEquals("yes", template.format(getResolver()));

        template = new NameTemplateParser().parse("${var1?yes:no}");
        try {
            assertEquals("no", template.format(getResolver()));
            fail("Expected exception");
        } catch (NameTemplateEvaluationException e) {
            // expected
        }

        template = new NameTemplateParser().parse("${nonexisting?yes:no}");
        try {
            assertEquals("no", template.format(getResolver()));
            fail("Expected exception");
        } catch (NameTemplateEvaluationException e) {
            // expected
        }
    }

    @Test
    public void testCondEmbedded() throws Exception {
        NameTemplate template = new NameTemplateParser().parse("prefix_${bool1?yes:no}_suffix");
        assertEquals("prefix_yes_suffix", template.format(getResolver()));
    }

    @Test
    public void testIncompleteExpr() throws Exception {
        NameTemplate template = new NameTemplateParser().parse("${");
        assertEquals("${", template.format(getResolver()));

        template = new NameTemplateParser().parse("x${x${x");
        assertEquals("x${x${x", template.format(getResolver()));

        template = new NameTemplateParser().parse("x${}");
        assertEquals("x${}", template.format(getResolver()));

        template = new NameTemplateParser().parse("x${?}");
        try {
            assertEquals("x${?}", template.format(getResolver()));
            fail("expected exception");
        } catch (NameTemplateEvaluationException e) {
            // expected
        }

        template = new NameTemplateParser().parse("x${a?}");
        try {
            assertEquals("x${a?}", template.format(getResolver()));
            fail("expected exception");
        } catch (NameTemplateEvaluationException e) {
            // expected
        }
    }

    public NameTemplateResolver getResolver() {
        Map<String, Object> context = new HashMap<String, Object>();
        context.put("var1", "hello");
        context.put("bool1", Boolean.TRUE);
        context.put("bool2", Boolean.FALSE);

        return new DefaultNameTemplateResolver(context);
    }
}
