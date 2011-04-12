package org.lilyproject.indexer.model.indexerconf.test;

import org.junit.Test;
import org.lilyproject.indexer.model.indexerconf.NameTemplate;
import org.lilyproject.indexer.model.indexerconf.NameTemplateEvaluationException;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class NameTemplateTest {
    @Test
    public void testLiteral() throws Exception {
        NameTemplate template = new NameTemplate("foobar");
        assertEquals("foobar", template.format(null));
    }

    @Test
    public void testVar() throws Exception {
        NameTemplate template = new NameTemplate("${var1}");
        assertEquals("hello", template.format(getContext()));
    }

    @Test
    public void testEmbeddedVar() throws Exception {
        NameTemplate template = new NameTemplate("prefix_${var1}_suffix");
        assertEquals("prefix_hello_suffix", template.format(getContext()));
    }

    @Test
    public void testCond() throws Exception {
        NameTemplate template = new NameTemplate("${bool1?yes:no}");
        assertEquals("yes", template.format(getContext()));

        template = new NameTemplate("${bool2?yes:no}");
        assertEquals("no", template.format(getContext()));

        template = new NameTemplate("${bool2?yes}");
        assertEquals("", template.format(getContext()));

        template = new NameTemplate("${bool1?yes}");
        assertEquals("yes", template.format(getContext()));

        template = new NameTemplate("${var1?yes:no}");
        try {
            assertEquals("no", template.format(getContext()));
            fail("Expected exception");
        } catch (NameTemplateEvaluationException e) {
            // expected
        }

        template = new NameTemplate("${nonexisting?yes:no}");
        try {
            assertEquals("no", template.format(getContext()));
            fail("Expected exception");
        } catch (NameTemplateEvaluationException e) {
            // expected
        }
    }

    @Test
    public void testCondEmbedded() throws Exception {
        NameTemplate template = new NameTemplate("prefix_${bool1?yes:no}_suffix");
        assertEquals("prefix_yes_suffix", template.format(getContext()));
    }

    @Test
    public void testIncompleteExpr() throws Exception {
        NameTemplate template = new NameTemplate("${");
        assertEquals("${", template.format(getContext()));

        template = new NameTemplate("x${x${x");
        assertEquals("x${x${x", template.format(getContext()));

        template = new NameTemplate("x${}");
        assertEquals("x${}", template.format(getContext()));

        template = new NameTemplate("x${?}");
        try {
            assertEquals("x${?}", template.format(getContext()));
            fail("expected exception");
        } catch (NameTemplateEvaluationException e) {
            // expected
        }

        template = new NameTemplate("x${a?}");
        try {
            assertEquals("x${a?}", template.format(getContext()));
            fail("expected exception");
        } catch (NameTemplateEvaluationException e) {
            // expected
        }
    }

    public Map<String, Object> getContext() {
        Map<String, Object> context = new HashMap<String, Object>();
        context.put("var1", "hello");
        context.put("bool1", Boolean.TRUE);
        context.put("bool2", Boolean.FALSE);
        return context;
    }
}
