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

import org.junit.Test;
import org.lilyproject.indexer.model.indexerconf.WildcardPattern;
import org.lilyproject.util.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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

        result = pattern.match("foo");
        assertTrue(result.getV1());
        assertEquals("", result.getV2());
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

        result = pattern.match("bar");
        assertTrue(result.getV1());
        assertEquals("", result.getV2());
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
    }

    @Test
    public void testSpecial() throws Exception {
        // A star by itself is not a wildcard
        WildcardPattern pattern = new WildcardPattern("*");
        Pair<Boolean, String> result = pattern.match("foo");
        assertTrue(result.getV1());
        assertEquals("foo", result.getV2());

        result = pattern.match("");
        assertTrue(result.getV1());
        assertEquals("", result.getV2());

        // first occurrence of star takes precedence
        pattern = new WildcardPattern("*foo*");
        result = pattern.match("barfoo*");
        assertTrue(result.getV1());
        assertEquals("bar", result.getV2());

        result = pattern.match("barfoobar");
        assertFalse(result.getV1());
        assertNull(result.getV2());
    }
}
