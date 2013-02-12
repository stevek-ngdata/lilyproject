/*
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
package org.kauriproject.template;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.kauriproject.template.el.GroovyExpression;
import org.kauriproject.util.xml.LocalDocumentBuilderFactory;
import org.kauriproject.xml.sax.Saxable;
import org.w3c.dom.Document;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

public class TemplateExecutionTest extends TemplateTestBase {

    public void testPlain() throws Exception {
        testFlow("/org/kauriproject/template/plain.xml", true);
    }

    public void testForeach() throws Exception {
        Map<String, Object> variables = new HashMap<String, Object>();
        String[] myarray = new String[] { "one", "two", "three" };
        List<String> mylist = new ArrayList<String>();
        mylist.add("item1");
        mylist.add("item2");
        mylist.add("item3");
        variables.put("mylist", mylist);
        variables.put("myarray", myarray);
        testFlow("/org/kauriproject/template/foreach.xml", variables, true);
    }

    public void testIf() throws Exception {
        testFlow("/org/kauriproject/template/if.xml", true);
    }

    public void testChoose() throws Exception {
        testFlow("/org/kauriproject/template/choose.xml", true);
    }

    public void testEL() throws Exception {
        // preload groovy
        assertNotNull("error loading groovy", new GroovyExpression("\"loaded groovy\"")
                .evaluate(new DefaultTemplateContext()));
        testFlow("/org/kauriproject/template/el.xml", true);
    }

    public void testELFunctions() throws Exception {
        testFlow("/org/kauriproject/template/elfunc.xml", true);
    }

    public void testMix() throws Exception {
        testFlow("/org/kauriproject/template/mix.xml", true);
    }

    public void testAttribute() throws Exception {
        testFlow("/org/kauriproject/template/attribute.xml", true);
    }

    public void testAttribute2() throws Exception {
        testFlow("/org/kauriproject/template/attribute2.xml", true);
    }

    public void testElement() throws Exception {
        testFlow("/org/kauriproject/template/element.xml", true);
    }

    public void testBig() throws Exception {
        testFlow("/org/kauriproject/template/big.xml", false);
        // test it two times in a row
        testFlow("/org/kauriproject/template/big.xml", true);
    }

    public void testTaglib() throws Exception {
        testFlow("/org/kauriproject/template/taglib/taglib.xml", true);
    }

    public void testInsert() throws Exception {
        testFlow("/org/kauriproject/template/insert.xml", true);
        testFlow("/org/kauriproject/template/insert_text.xml", true);
        testFlow("/org/kauriproject/template/insert_html.xml", true);
        testFlow("/org/kauriproject/template/insert_htmlfragment.xml", true);
    }

    public void testVariable() throws Exception {
        Map<String, Object> variables = new HashMap<String, Object>();
        Calendar cal = Calendar.getInstance();
        cal.set(2008, 1, 8, 11, 55, 0);
        variables.put("mydate", cal.getTime());
        testFlow("/org/kauriproject/template/variable.xml", variables, true);
    }

    public void testComment() throws Exception {
        testFlow("/org/kauriproject/template/comment.xml", true);
    }

    public void testMacro() throws Exception {
        testFlow("/org/kauriproject/template/macro.xml", true);
    }

    public void testInclude() throws Exception {
        testFlow("/org/kauriproject/template/include.xml", true);
    }

    public void testImport() throws Exception {
        testFlow("/org/kauriproject/template/import.xml", true);
    }

    public void testInheritance() throws Exception {
        testFlow("/org/kauriproject/template/inherit.xml", true);
        testFlow("/org/kauriproject/template/inherit_base.xml", true);
        testFlow("/org/kauriproject/template/inherit_multiple.xml", true);
    }

    public void testInit() throws Exception {
        testFlow("/org/kauriproject/template/init_flat.xml", true);
        testFlow("/org/kauriproject/template/init_special.xml", true);
        // special should yield the same result as flat
        String result1 = loadXML("/org/kauriproject/template/init_flat_result.xml", true);
        String result2 = loadXML("/org/kauriproject/template/init_special_result.xml", true);
        assertEquals("Both results should be equal to get a useful test.", result1, result2);
    }

    public void testDropPrefixMappings() throws Exception {
        // test drop mapping
        testFlow("/org/kauriproject/template/dropprefix1.xml", true);

        // test drop mapping leaves non-dropped namespaces in place
        testFlow("/org/kauriproject/template/dropprefix2.xml", true);

        // test drop all namespaces
        testFlow("/org/kauriproject/template/dropprefix3.xml", true);

        // test drop mapping which is still used in the document
        testFlow("/org/kauriproject/template/dropprefix4.xml", true);

        // test drop mapping which is still used in the document, attrs with same local name
        testFlow("/org/kauriproject/template/dropprefix4a.xml", true);

        // test drop default namespace
        testFlow("/org/kauriproject/template/dropprefix5.xml", true);
    }

    public void testWhitespace() throws Exception {
        testFlow("/org/kauriproject/template/whitespace.xml", null, true, false);
    }

    public void testVariableFromSrc() throws Exception {
        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("overwriteTest1", "overwriteTest1 - external value");
        parameters.put("overwriteTest2", "overwriteTest2 - external value");
        testFlow("/org/kauriproject/template/variable_from_src.xml", parameters, true);
    }

    public void testXmlVariables() throws Exception {
        Map<String, Object> variables = new HashMap<String, Object>();

        Document document = LocalDocumentBuilderFactory.newDocument();
        document.appendChild(document.createElement("domElement"));
        variables.put("document", document);

        Saxable saxable = new Saxable() {
            public void toSAX(ContentHandler contentHandler) throws SAXException {
                contentHandler.startElement("", "saxable", "saxable", new AttributesImpl());
                String message = "hi";
                contentHandler.characters(message.toCharArray(), 0, message.length());
                contentHandler.endElement("", "saxable", "saxable");
            }
        };
        variables.put("saxable", saxable);

        testFlow("/org/kauriproject/template/xmlvariables.xml", variables, true);
    }
    
    public void testJsonVariables() throws Exception {
        testFlow("/org/kauriproject/template/jsonvariables.xml", true);
    }

    public void testModeHandling() throws Exception {
        testFlow("/org/kauriproject/template/mode_handling.xml", true);
    }

}
