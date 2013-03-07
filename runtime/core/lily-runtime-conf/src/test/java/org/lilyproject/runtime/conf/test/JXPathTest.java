/*
 * Copyright 2013 NGDATA nv
 * Copyright 2008 Outerthought bvba and Schaubroeck nv
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
package org.lilyproject.runtime.conf.test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;
import org.apache.commons.jxpath.JXPathContext;
import org.apache.commons.jxpath.Pointer;
import org.lilyproject.runtime.conf.Conf;
import org.lilyproject.runtime.conf.XmlConfBuilder;

public class JXPathTest extends TestCase {
    public void testIt() throws Exception {
        String path = "jxpathconf.xml";
        Conf conf = XmlConfBuilder.build(getClass().getResourceAsStream(path), path);

        JXPathContext context = JXPathContext.newContext(conf);

        assertEquals("Venus", context.getValue("planet"));
        assertEquals("Mars", context.getValue("@planet"));

        assertEquals("5", context.getValue("/things/thing[@name='Book']/@quantity"));
        assertEquals("50", context.getValue("/things/thing[@name='Bicycle']/@quantity"));

        assertEquals("Book", context.getValue("/things/thing[1]/@name"));

        assertEquals("Bicycle", context.getValue("/things/thing[2]/@name"));
        assertEquals("Bicycle", context.getValue("/things/thing[last()]/@name"));

        List<Conf> things = new ArrayList<Conf>();
        Iterator thingsIt = context.iteratePointers("things/thing[position() < 3]");
        while (thingsIt.hasNext()) {
            Pointer pointer = (Pointer)thingsIt.next();
            assertTrue(pointer.getNode() instanceof Conf);
            things.add((Conf)pointer.getNode());
        }
        assertEquals(2, things.size());
    }
}
