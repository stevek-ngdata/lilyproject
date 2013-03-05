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

import junit.framework.TestCase;
import org.lilyproject.runtime.conf.Conf;
import org.lilyproject.runtime.conf.XmlConfBuilder;
import org.lilyproject.runtime.conf.ConfException;
import org.xml.sax.SAXException;

import java.io.InputStream;

public class ConfModelTest extends TestCase {
    public void testConfig1() throws Exception {
        String configPath = "config1.xml";
        InputStream configStream = getClass().getResourceAsStream(configPath);

        Conf config = XmlConfBuilder.build(configStream, configPath);
        assertEquals("value1", config.getAttribute("att1"));
        assertEquals("value2", config.getAttribute("att2"));

        assertEquals(3, config.getChildren().size());
        assertEquals(2, config.getChildren("element").size());

        for (Conf childConf : config.getChildren("element")) {
            assertEquals("element", childConf.getName());
            assertEquals("abc", childConf.getValue());
        }

        assertEquals(2, config.getChild("parent").getChildren("child").size());

        for (Conf childConf : config.getChild("parent").getChildren("child")) {
            assertEquals("def", childConf.getValue());
        }

        assertEquals(0, config.getChild("parent").getChild("child").getChildren().size());

        assertNotNull(config.getChild("nonexistingchild"));
        assertNull(config.getChild("nonexistingchild", false));

        // Getting the value of a node without a value should throw an error
        try {
            config.getChild("nonexistingchild").getValue();
            fail("expected exception");
        } catch (ConfException e) { /* ignore */ }

        assertEquals("zit", config.getChild("nonexistingchild").getValue("zit"));        
    }

    /**
     * Test that mixed content is not allowed.
     */
    public void testConfig2() throws Exception {
        String configPath = "config2.xml";
        InputStream configStream = getClass().getResourceAsStream(configPath);

        try {
            XmlConfBuilder.build(configStream, configPath);
            fail("expected exception");
        } catch (SAXException e) {
            assertTrue(e.getMessage().toLowerCase().indexOf("mixed content") != -1);
        }
    }

    /**
     * Test that namespaced root element is not allowed.
     */
    public void testConfig3() throws Exception {
        String configPath = "config3.xml";
        InputStream configStream = getClass().getResourceAsStream(configPath);

        try {
            XmlConfBuilder.build(configStream, configPath);
            fail("expected exception");
        } catch (SAXException e) {
            assertTrue(e.getMessage().indexOf("namespace") > 0);
        }
    }

    /**
     * Test that namespaced elements and attributes are ignored.
     */
    public void testConfig4() throws Exception {
        String configPath = "config4.xml";
        InputStream configStream = getClass().getResourceAsStream(configPath);

        XmlConfBuilder.build(configStream, configPath);
    }

    /**
     * Test the getAsXXX methods.
     */
    public void testConfig5() throws Exception {
        String configPath = "config5.xml";
        InputStream configStream = getClass().getResourceAsStream(configPath);

        Conf config = XmlConfBuilder.build(configStream, configPath);

        assertTrue(config.getChild("boolean").getValueAsBoolean());
        assertEquals(5, config.getChild("int").getValueAsInteger());
        assertEquals(6l, config.getChild("long").getValueAsLong());
        assertEquals(3.3f, config.getChild("float").getValueAsFloat(), 0.0001f);
        assertEquals(5.5d, config.getChild("double").getValueAsDouble(), 0.0001d);

        // Test fallback to default
        assertTrue(config.getChild("boolean2").getValueAsBoolean(true));
        assertEquals(new Integer(5), config.getChild("int2").getValueAsInteger(5));
        assertEquals(new Long(6l), config.getChild("long2").getValueAsLong(6l));
        assertEquals(3.3f, config.getChild("float2").getValueAsFloat(3.3f), 0.0001f);
        assertEquals(5.5d, config.getChild("double2").getValueAsDouble(5.5d), 0.0001d);

        // Test attributes
        assertTrue(config.getAttributeAsBoolean("boolean"));
        assertEquals(5, config.getAttributeAsInteger("int"));
        assertEquals(6l, config.getAttributeAsLong("long"));
        assertEquals(3.3f, config.getAttributeAsFloat("float"), 0.0001f);
        assertEquals(5.5d, config.getAttributeAsDouble("double"), 0.0001d);

        // Test attribute fallback to default
        assertTrue(config.getAttributeAsBoolean("boolean2", true));
        assertEquals(new Integer(5), config.getAttributeAsInteger("int2", 5));
        assertEquals(new Long(6l), config.getAttributeAsLong("long2", 6l));
        assertEquals(3.3f, config.getAttributeAsFloat("float2", 3.3f), 0.0001f);
        assertEquals(5.5d, config.getAttributeAsDouble("double2", 5.5d), 0.0001d);
    }
}
