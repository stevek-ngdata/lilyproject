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

import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.BufferedReader;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kauriproject.template.source.ClasspathSourceResolver;
import org.kauriproject.template.source.Source;
import org.kauriproject.template.source.SourceResolver;
import org.kauriproject.template.taglib.Taglib;
import org.kauriproject.xml.sax.XmlConsumer;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.SAXException;

public class TemplateTestBase extends TestCase {

    // development flag - log durations and fail if too slow
    protected static final boolean TEST_DURATION = false;

    protected static Log log = LogFactory.getLog(TemplateTestBase.class);

    private static final String ENCODING = "UTF-8";

    // Treshold (in seconds) for allowed duration of execution of each test template.
    protected double executionTreshold;

    // Treshold (in seconds) for allowed duration of building of each test template.
    protected double buildTreshold;

    // Treshold (in seconds) for allowed duration of entire build/exec/handle process of each template.
    protected double flowTreshold;

    private SAXParser parser;
    private SourceResolver sourceResolver;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        // setup parser
        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.setFeature("http://xml.org/sax/features/namespace-prefixes", true);
        parser = factory.newSAXParser();
        sourceResolver = new ClasspathSourceResolver();
    }

    protected void initTresholds(String template, boolean prettyFormat) throws Exception {
        long start = new Date().getTime();
        // load template
        loadXML(template, prettyFormat);
        long end = new Date().getTime();
        // check duration
        double duration = getDuration(start, end);
        log.info("initial loading: " + duration + " sec.");

        executionTreshold = Math.max(0.05, duration);
        buildTreshold = executionTreshold * 2;
        flowTreshold = executionTreshold * 3;
    }

    /**
     * Test execution of a compiled template.
     */
    protected void executeTemplate(CompiledTemplate compiledTemplate, TemplateResult result,
            Map<String, Object> variables, double treshold) throws SAXException {
        // setup
        TemplateContext context = new DefaultTemplateContext();
        if (variables != null) {
            context.putAll(variables);
        }
        ExecutionContext execContext = new ExecutionContext();
        execContext.setTemplateContext(context);
        execContext.setSourceResolver(sourceResolver);
        DefaultTemplateExecutor executor = new DefaultTemplateExecutor();

        // monitor duration
        long start = new Date().getTime();
        // execute template
        executor.execute(compiledTemplate, null, execContext, result);
        long end = new Date().getTime();

        // check if there is a templateresult
        assertNotNull("TemplateResult should not be null.", result);

        // check duration
        double duration = getDuration(start, end);
        if (TEST_DURATION) {
            log.info("duration execution: " + duration + " sec.");
            assertTrue("Execution was too slow: " + duration + " sec.", duration < treshold);
        }
    }

    /**
     * Test the build/compilation process of a template.
     */
    protected CompiledTemplate buildTemplate(String template, double treshold) throws Exception {
        // setup
        DefaultTemplateBuilder builder = new DefaultTemplateBuilder(null, new DefaultTemplateService(), false);
        builder.setTaglibs(loadTaglibs());
        // load
        Source resource = sourceResolver.resolve(template);
        // monitor duration
        long start = new Date().getTime();
        // build template
        CompiledTemplate compiledTemplate = builder.buildTemplate(resource);
        long end = new Date().getTime();

        // check if there is a compiledTemplate
        assertNotNull("CompiledTemplate should not be null", compiledTemplate);

        // check duration
        double duration = getDuration(start, end);
        if (TEST_DURATION) {
            log.info("duration build: " + duration + " sec.");
            assertTrue("Building was too slow: " + duration + " sec.", duration < treshold);
        }

        return compiledTemplate;
    }

    /**
     * Load, build and execute template and optionally check the result.
     */
    protected void testFlow(String template, boolean check) throws Exception {
        testFlow(template, null, check);
    }

    protected void testFlow(String template, Map<String, Object> variables, boolean check) throws Exception {
        testFlow(template, variables, check, true);
    }

    /**
     * Load, build and execute template and optionally check the result.
     */
    protected void testFlow(String template, Map<String, Object> variables, boolean check, boolean prettyFormat)
            throws Exception {
        // init tresholds for duration
        if (TEST_DURATION) {
            initTresholds(template, prettyFormat);
        }

        // monitor duration
        long start = new Date().getTime();

        // load and build template
        CompiledTemplate compiledTemplate = buildTemplate(template, buildTreshold);

        // execute template
        ByteArrayOutputStream outputResult = new ByteArrayOutputStream();
        TemplateResult result;
        if (prettyFormat) {
            result = new TemplateResultImpl(new KauriSaxHandler.NamespaceAsAttributes(
                    new TestHandler(outputResult)));
        } else {
            result = new TemplateResultImpl(new KauriSaxHandler(outputResult));
        }
        executeTemplate(compiledTemplate, result, variables, executionTreshold);

        // flush result
        result.flush();
        long end = new Date().getTime();

        // check duration
        double duration = getDuration(start, end);
        if (TEST_DURATION) {
            log.info("duration flow: " + duration + " sec.\n");
            assertTrue("Flow was too slow: " + duration + " sec.", duration < flowTreshold);
        }

        // check result
        if (check) {
            checkResult(template, outputResult, prettyFormat);
        }
    }

    /**
     * Check if the templateresult meets our expectations.
     */
    protected void checkResult(String filename, ByteArrayOutputStream bos, boolean prettyFormat) throws Exception {
        String expectedFile = filename.replace(".xml", "_result.xml");
        String expectedString = loadXML(expectedFile, prettyFormat);
        String actualString = bos.toString(ENCODING);
        if (!expectedString.equals(actualString)) {
            System.out.println("=======================================================================");
            System.out.println("Execution of " + filename + " does not match " + expectedFile);
            System.out.println("-> Output:");
            System.out.println(actualString);
            System.out.println("-> Expected:");
            System.out.println(expectedString);
        }
        assertEquals("Actual XML result does not equal our expected XML.", expectedString, actualString);
    }

    protected String loadXML(String filename, boolean prettyFormat) throws Exception {
        Source source = sourceResolver.resolve(filename);
        if (prettyFormat) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            XmlConsumer consumer = new TestHandler(bos);
            XMLReader reader = parser.getXMLReader();
            reader.setContentHandler(consumer);
            reader.setProperty("http://xml.org/sax/properties/lexical-handler", consumer);
            reader.parse(new InputSource(source.getInputStream()));
            String xmlString = bos.toString(ENCODING);

            return xmlString;
        } else {
            BufferedReader reader = new BufferedReader(new InputStreamReader(source.getInputStream(), "UTF-8"));
            StringBuilder content = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (content.length() > 0)
                    content.append("\n");
                content.append(line);
            }
            reader.close();
            return content.toString();
        }
    }

    /**
     * Compute the difference (in seconds) between two time values (expressed in milliseconds).
     */
    protected double getDuration(final long start, final long end) {
        return ((end - start) / 1000.0);
    }

    private Map<String, Taglib> loadTaglibs() {
        Map<String, Taglib> taglibs = new HashMap<String, Taglib>();
        Class<?> taglibClass;
        Taglib taglib;
        try {
            taglibClass = Class.forName("org.kauriproject.template.taglib.TestTaglib");
            taglib = (Taglib) taglibClass.newInstance();
            if (!taglib.getNamespace().equals(TemplateBuilder.NAMESPACE_KTL)) {
                taglibs.put(taglib.getNamespace(), taglib);
            } else {
                log.error("Forbidden to use the default kauri template namespace for taglibs.");
            }

        } catch (ClassNotFoundException ex) {
            log.error("error loading taglib: " + ex);
            ex.printStackTrace();
        } catch (InstantiationException ex) {
            log.error("error loading taglib: " + ex);
            ex.printStackTrace();
        } catch (IllegalAccessException ex) {
            log.error("error loading taglib: " + ex);
            ex.printStackTrace();
        } catch (ClassCastException ex) {
            log.error("error loading taglib: " + ex);
            ex.printStackTrace();
        }
        return taglibs;

    }

}
