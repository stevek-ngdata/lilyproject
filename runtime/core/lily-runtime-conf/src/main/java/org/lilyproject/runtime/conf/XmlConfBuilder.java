package org.lilyproject.runtime.conf;

import org.xml.sax.*;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.runtime.util.xml.LocalSAXParserFactory;
import org.lilyproject.runtime.util.location.Location;
import org.lilyproject.runtime.util.location.LocationImpl;
import org.lilyproject.runtime.util.io.IOUtils;

import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.util.List;
import java.util.ArrayList;

public class XmlConfBuilder {
    private static final String CONFIG_NAMESPACE = "http://lilyproject.org/configuration";

    public static ConfImpl build(File file) throws IOException, SAXException, ParserConfigurationException {
        InputStream is = null;
        try {
            is = new BufferedInputStream(new FileInputStream(file));
            return build(is, file.getAbsolutePath());
        } finally {
            IOUtils.closeQuietly(is, file.getAbsolutePath());
        }
    }

    public static ConfImpl build(InputStream is, String location) throws IOException, SAXException,
            ParserConfigurationException
    {
        InputSource inputSource = new InputSource(is);
        inputSource.setSystemId(location);

        XMLReader xmlReader = LocalSAXParserFactory.newXmlReader();
        ConfigurationHandler configHandler = new ConfigurationHandler();
        xmlReader.setContentHandler(configHandler);
        xmlReader.parse(inputSource);

        return configHandler.rootConfig;
    }

    private static class ConfigurationHandler implements ContentHandler {
        private List<ConfImpl> configs = new ArrayList<ConfImpl>();
        private ConfImpl rootConfig;
        private List<StringBuilder> charBuffers = new ArrayList<StringBuilder>();
        private Locator locator;

        public void setDocumentLocator(Locator locator) {
            this.locator = locator;
        }

        public void startDocument() throws SAXException {
        }

        public void endDocument() throws SAXException {
        }

        public void startPrefixMapping(String prefix, String uri) throws SAXException {
        }

        public void endPrefixMapping(String prefix) throws SAXException {
        }

        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            if (uri.equals("")) {
                Location location = locator == null ? Location.UNKNOWN : new LocationImpl(null, locator.getSystemId(),
                        locator.getLineNumber(), locator.getColumnNumber());
                ConfImpl config = new ConfImpl(localName, location);
                configs.add(config);

                for (int i = 0; i < atts.getLength(); i++) {
                    if (atts.getURI(i).equals("")) {
                        config.addAttribute(atts.getLocalName(i), atts.getValue(i));
                    } else if (atts.getURI(i).equals(CONFIG_NAMESPACE)) {
                        if (atts.getLocalName(i).equals("inherit")) {
                            String inheritString = atts.getValue(i);
                            ConfImpl.Inheritance inherit;

                            if (inheritString.equals("none")) {
                                inherit = ConfImpl.Inheritance.NONE;
                            } else if (inheritString.equals("shallow")) {
                                inherit = ConfImpl.Inheritance.SHALLOW;
                            } else if (inheritString.equals("deep")) {
                                inherit = ConfImpl.Inheritance.DEEP;
                            } else {
                                throw new SAXException("Invalid value in attribute " + atts.getQName(i)
                                        + ": \"" + inheritString + "\", should be one of: none, shallow, deep. At "
                                        + location);
                            }

                            config.setInheritance(inherit);
                        } else if (atts.getLocalName(i).equals("inheritKey")) {
                            config.setInheritConstraint(atts.getValue(i));
                        } else {
                            throw new SAXException("Unsupported attribute: " + atts.getQName(i) + " at " + location);
                        }
                    }
                }

                charBuffers.add(new StringBuilder());
            } else if (configs.size() == 0) {
                throw new SAXException("Root element of a configuration file should not be in a namespace.");
            }
        }

        public void endElement(String uri, String localName, String qName) throws SAXException {
            if (uri.equals("")) {
                ConfImpl config = configs.remove(configs.size() - 1);

                String textualContent = charBuffers.remove(charBuffers.size() - 1).toString().trim();
                if (textualContent.length() > 0) {
                    if (config.getChildren().size() > 0) {
                        throw new SAXException("Mixed content is not allowed in configuration element "
                                + config.getName() + " at " + config.getLocation());
                    }
                    config.setValue(textualContent);
                }

                if (configs.size() > 0) {
                    configs.get(configs.size() - 1).addChild(config);
                } else {
                    rootConfig = config;
                }
            }
        }

        public void characters(char[] ch, int start, int length) throws SAXException {
            charBuffers.get(charBuffers.size() - 1).append(ch, start, length);
        }

        public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        }

        public void processingInstruction(String target, String data) throws SAXException {
        }

        public void skippedEntity(String name) throws SAXException {
        }
    }
}
