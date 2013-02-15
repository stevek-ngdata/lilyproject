package org.lilyproject.runtime.conf.test;

import junit.framework.TestCase;
import org.apache.commons.jxpath.JXPathContext;
import org.lilyproject.runtime.conf.XmlConfBuilder;
import org.lilyproject.runtime.conf.ConfImpl;

public class InheritanceTest extends TestCase {
    public void testInheritance1() throws Exception {
        ConfImpl parent = loadConf("inherit1_parent.xml");

        // Child 1
        {
            ConfImpl child = loadConf("inherit1_child1.xml");
            child.inherit(parent);

            JXPathContext context = JXPathContext.newContext(child);
            assertEquals(1, context.getValue("count(properties)", Integer.class));
            assertEquals(3, context.getValue("count(properties/property)", Integer.class));

            assertEquals("value1", context.getValue("properties/property[@key='key1']"));
            assertEquals("value2 - altered", context.getValue("properties/property[@key='key2']"));
            assertEquals("value3", context.getValue("properties/property[@key='key3']"));
        }

        // Child 2
        {
            ConfImpl child = loadConf("inherit1_child2.xml");
            child.inherit(parent);
            
            JXPathContext context = JXPathContext.newContext(child);
            assertEquals(1, context.getValue("count(properties)", Integer.class));
            assertEquals(4, context.getValue("count(properties/property)", Integer.class));

            assertEquals("value1", context.getValue("properties/property[@key='key1']"));
            assertEquals("value2 - altered", context.getValue("properties/property[@key='key2'][1]"));
            assertEquals("value2", context.getValue("properties/property[@key='key2'][2]"));
            assertEquals("value3", context.getValue("properties/property[@key='key3']"));
        }

        // Child 3
        {
            ConfImpl child = loadConf("inherit1_child3.xml");
            child.inherit(parent);

            JXPathContext context = JXPathContext.newContext(child);
            assertEquals(1, context.getValue("count(properties)", Integer.class));
            assertEquals(1, context.getValue("count(properties/property)", Integer.class));

            assertEquals("value3", context.getValue("properties/property[@key='key3']"));
        }

        // Child 4
        {
            ConfImpl child = loadConf("inherit1_child4.xml");
            child.inherit(parent);

            JXPathContext context = JXPathContext.newContext(child);
            assertEquals(0, context.getValue("count(*)", Integer.class));
        }
    }

    public void testInheritance2() throws Exception {
        ConfImpl parent = loadConf("inherit2_parent.xml");

        // Child 1
        {
            ConfImpl child = loadConf("inherit2_child1.xml");
            child.inherit(parent);

            JXPathContext context = JXPathContext.newContext(child);
            assertEquals(3, context.getValue("count(*)", Integer.class));

            assertEquals("val1", context.getValue("@attr1"));
            assertEquals("val2 - altered", context.getValue("@attr2"));

            assertEquals("ee", context.getValue("c/e"));

            context.setLenient(true);
            assertEquals(null, context.getValue("c/d"));
        }
    }

    private ConfImpl loadConf(String name) throws Exception {
        return XmlConfBuilder.build(getClass().getResourceAsStream(name), name);
    }
}
