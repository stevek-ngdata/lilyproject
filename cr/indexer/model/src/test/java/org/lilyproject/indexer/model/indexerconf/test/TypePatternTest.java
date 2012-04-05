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
import org.lilyproject.indexer.model.indexerconf.TypePattern;

import static org.junit.Assert.*;

public class TypePatternTest {
    @Test
    public void testNameWildcards() throws Exception {
        TypePattern pattern = new TypePattern("STR*");

        assertTrue(pattern.matches("STRING"));
        assertFalse(pattern.matches("STING"));

        pattern = new TypePattern("STR*<STR*>");
        assertTrue(pattern.matches("STRING<STRONG>"));

        pattern = new TypePattern("STR*<STR*<*>>");
        assertTrue(pattern.matches("STRING<STRONG<LONG>>"));
        assertTrue(pattern.matches("STRING<STRONG>"));

        // wildcards in names will mostly be used for the record type argument for records
        pattern = new TypePattern("RECORD<{namespace}*>");
        assertTrue(pattern.matches("RECORD<{namespace}foo>"));
        assertFalse(pattern.matches("RECORD<{othernamespace}foo>"));
    }

    @Test
    public void testArgumentWildcards() throws Exception {
        TypePattern pattern = new TypePattern("LIST<STRING>");

        assertTrue(pattern.matches("LIST<STRING>"));
        assertFalse(pattern.matches("LIST<LONG>"));
        assertFalse(pattern.matches("LIST"));
        assertFalse(pattern.matches("LIST<LIST<STRING>>"));
        assertFalse(pattern.matches("FOO"));

        // no type arg
        pattern = new TypePattern("STRING");

        assertTrue(pattern.matches("STRING"));
        assertFalse(pattern.matches("STRING<STRING>"));

        // optional 1 nested type argument
        pattern = new TypePattern("LIST<*>");

        assertTrue(pattern.matches("LIST"));
        assertTrue(pattern.matches("LIST<STRING>"));
        assertFalse(pattern.matches("LIST<LIST<STRING>>"));

        // this shows this can also match nested lists, which may go a bit beyond the purpose as this kind
        // of matching should serve to guarantee 1-level nested types. However, in practice this won't occur
        // since a list always has a type argument.
        assertTrue(pattern.matches("LIST<LIST>"));

        // exactly 1 nested type argument
        pattern = new TypePattern("LIST<+>");

        assertTrue(pattern.matches("LIST<STRING>"));
        assertFalse(pattern.matches("LIST"));
        assertFalse(pattern.matches("LIST<LIST<STRING>>"));

        // optionally any number of nested type arguments, including 0
        pattern = new TypePattern("LIST<**>");

        assertTrue(pattern.matches("LIST"));
        assertFalse(pattern.matches("STRING"));
        assertTrue(pattern.matches("LIST<STRING>"));
        assertTrue(pattern.matches("LIST<LIST<STRING>>"));
        assertTrue(pattern.matches("LIST<LIST<PATH<STRING>>>"));

        // optionally any number of nested type arguments, but at least 1
        pattern = new TypePattern("LIST<++>");

        assertFalse(pattern.matches("STRING"));
        assertTrue(pattern.matches("LIST<STRING>"));
        assertTrue(pattern.matches("LIST<LIST<STRING>>"));
        assertTrue(pattern.matches("LIST<LIST<PATH<STRING>>>"));

        // exactly 1 nested type argument at a deeper level
        pattern = new TypePattern("LIST<LIST<+>>");

        assertTrue(pattern.matches("LIST<LIST<STRING>>"));
        assertTrue(pattern.matches("LIST<LIST<LONG>>"));
        assertFalse(pattern.matches("LIST<LIST<LIST<LONG>>>"));
    }

    @Test
    public void testMultiPatterns() throws Exception {
        TypePattern pattern = new TypePattern("STRING,LONG,DATE");

        assertTrue(pattern.matches("STRING"));
        assertTrue(pattern.matches("LONG"));
        assertTrue(pattern.matches("DATE"));
        assertFalse(pattern.matches("RECORD"));


        pattern = new TypePattern("STRING,LIST<STRING>");
        assertTrue(pattern.matches("STRING"));
        assertTrue(pattern.matches("LIST<STRING>"));
        assertFalse(pattern.matches("RECORD"));
    }

    @Test
    public void testMore() throws Exception {
        // match things and lists of things
        TypePattern pattern = new TypePattern("*,LIST<+>");

        assertTrue(pattern.matches("STRING"));
        assertTrue(pattern.matches("DATE"));
        assertTrue(pattern.matches("LIST<STRING>"));
        assertFalse(pattern.matches("LIST<LIST<STRING>>"));
        assertFalse(pattern.matches("STRING<STRING>"));
    }

    @Test
    public void testRoundBracket() throws Exception {
        // Because angle brackets are annoying to write in XML, one can use round brackets too.
        TypePattern pattern = new TypePattern("LIST(LIST(RECORD(*)))");

        assertTrue(pattern.matches("LIST(LIST(RECORD))"));
        assertTrue(pattern.matches("LIST(LIST(RECORD(foobar)))"));
    }
}
