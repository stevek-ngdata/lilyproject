package org.lilyproject.runtime.conf.test;

import junit.framework.TestCase;
import org.apache.commons.jxpath.JXPathContext;
import org.apache.commons.jxpath.Pointer;
import org.lilyproject.runtime.conf.Conf;
import org.lilyproject.runtime.conf.XmlConfBuilder;

import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

public class JXPathTest extends TestCase {
    public void testIt() throws Exception {
        String path = "org/lilyproject/conf/test/jxpathconf.xml";
        Conf conf = XmlConfBuilder.build(getClass().getClassLoader().getResourceAsStream(path), path);

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
