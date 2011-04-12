package org.lilyproject.indexer.model.indexerconf.test;

import org.junit.Test;
import org.lilyproject.indexer.model.indexerconf.WildcardPattern;
import org.lilyproject.util.Pair;

import static org.junit.Assert.*;

public class WildcardPatternTest {
    @Test
    public void testStartsWith() throws Exception {
        WildcardPattern pattern = new WildcardPattern("foo*");
        Pair<Boolean, String> result = pattern.match("foobar");
        assertTrue(result.getV1());
        assertEquals("bar", result.getV2());

        result = pattern.match("fobar");
        assertFalse(result.getV1());
        assertNull(result.getV2());
    }

    @Test
    public void testEndsWith() throws Exception {
        WildcardPattern pattern = new WildcardPattern("*bar");
        Pair<Boolean, String> result = pattern.match("foobar");
        assertTrue(result.getV1());
        assertEquals("foo", result.getV2());

        result = pattern.match("fooba");
        assertFalse(result.getV1());
        assertNull(result.getV2());
    }

    @Test
    public void testEquals() throws Exception {
        WildcardPattern pattern = new WildcardPattern("foobar");
        Pair<Boolean, String> result = pattern.match("foobar");
        assertTrue(result.getV1());
        assertNull(result.getV2());

        // A star at any other position than end or start is not recognized as a wildcard
        pattern = new WildcardPattern("foo*bar");
        result = pattern.match("foo*bar");
        assertTrue(result.getV1());
        assertNull(result.getV2());

        result = pattern.match("foo1bar");
        assertFalse(result.getV1());

        // A star by itself is not a wildcard
        pattern = new WildcardPattern("*");
        result = pattern.match("*");
        assertTrue(result.getV1());
        assertNull(result.getV2());

        result = pattern.match("foo");
        assertFalse(result.getV1());
    }
}
