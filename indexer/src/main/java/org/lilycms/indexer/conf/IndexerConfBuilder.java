package org.lilycms.indexer.conf;

import org.lilycms.repository.api.FieldTypeNotFoundException;
import org.lilycms.repository.api.QName;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repoutil.VersionTag;
import org.lilycms.util.location.LocationAttributes;
import org.lilycms.util.xml.DocumentHelper;
import org.lilycms.util.xml.LocalXPathExpression;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.InputStream;
import java.util.*;

// TODO: add some validation of the XML?

// Terminology: the word "field" is usually used for a field from a repository record, while
// the term "index field" is usually used for a field in the index, though sometimes these
// are also just called field.
public class IndexerConfBuilder {
    private static LocalXPathExpression INDEX_CASES =
            new LocalXPathExpression("/indexer/cases/case");

    private static LocalXPathExpression INDEX_FIELDS =
            new LocalXPathExpression("/indexer/indexFields/indexField");

    private Document doc;

    private IndexerConf conf;

    private Repository repository;
    private TypeManager typeManager;

    private IndexerConfBuilder() {
        // prevents instantiation
    }

    public static IndexerConf build(InputStream is, Repository repository) throws IndexerConfException {
        Document doc;
        try {
            doc = DocumentHelper.parse(is);
        } catch (Exception e) {
            throw new IndexerConfException("Error parsing supplied configuration.", e);
        }
        return new IndexerConfBuilder().build(doc, repository);
    }

    private IndexerConf build(Document doc, Repository repository) throws IndexerConfException {
        this.doc = doc;
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
        this.conf = new IndexerConf();

        try {
            buildCases();
            buildIndexFields();
        } catch (Exception e) {
            throw new IndexerConfException("Error in the configuration.", e);
        }

        return conf;
    }

    private void buildCases() throws Exception {
        List<Element> cases = INDEX_CASES.get().evalAsNativeElementList(doc);
        for (Element caseEl : cases) {
            // TODO will need resolving of the QName
            String recordType = DocumentHelper.getAttribute(caseEl, "recordType", true);
            String vtagsSpec = DocumentHelper.getAttribute(caseEl, "vtags", false);
            boolean indexVersionless = DocumentHelper.getBooleanAttribute(caseEl, "indexVersionless", false);

            Map<String, String> varPropsPattern = parseVariantPropertiesPattern(caseEl);
            Set<String> vtags = parseVersionTags(vtagsSpec);

            IndexCase indexCase = new IndexCase(recordType, varPropsPattern, vtags, indexVersionless);
            conf.addIndexCase(indexCase);
        }
    }

    private Map<String, String> parseVariantPropertiesPattern(Element caseEl) throws Exception {
        String variant = DocumentHelper.getAttribute(caseEl, "variant", false);

        Map<String, String> varPropsPattern = new HashMap<String, String>();

        if (variant == null)
            return varPropsPattern;

        String[] props = variant.split(",");
        for (String prop : props) {
            prop = prop.trim();
            if (prop.length() > 0) {
                int eqPos = prop.indexOf("=");
                if (eqPos != -1) {
                    String propName = prop.substring(0, eqPos);
                    String propValue = prop.substring(eqPos + 1);
                    if (propName.equals("*")) {
                        throw new IndexerConfException(String.format("Error in variant attribute: the character '*' " +
                                "can only be used as wildcard, not as variant dimension name, attribute = %1$s, at: %2$s",
                                variant, LocationAttributes.getLocation(caseEl)));
                    }
                    varPropsPattern.put(propName, propValue);
                } else {
                    varPropsPattern.put(prop, null);
                }
            }
        }

        return varPropsPattern;
    }

    private Set<String> parseVersionTags(String vtagsSpec) throws IndexerConfException {
        Set<String> vtags = new HashSet<String>();

        if (vtagsSpec == null)
            return vtags;

        String[] tags = vtagsSpec.split(",");
        for (String tag : tags) {
            tag = tag.trim();
            if (tag.length() > 0) {
                try {
                    vtags.add(typeManager.getFieldTypeByName(VersionTag.qname(tag)).getId());
                } catch (FieldTypeNotFoundException e) {
                    throw new IndexerConfException("unknown vtag used in indexer configuration: " + tag);
                }
            }
        }

        return vtags;
    }

    private void buildIndexFields() throws Exception {
        List<Element> indexFields = INDEX_FIELDS.get().evalAsNativeElementList(doc);
        for (Element indexFieldEl : indexFields) {
            String name = DocumentHelper.getAttribute(indexFieldEl, "name", true);
            validateName(name);
            Element valueEl = DocumentHelper.getElementChild(indexFieldEl, "value", true);
            Value value = buildValue(valueEl);

            IndexField indexField = new IndexField(name, value);
            conf.addIndexField(indexField);
        }
    }

    private void validateName(String name) throws IndexerConfException {
        if (name.startsWith("@@")) {
            throw new IndexerConfException("names starting with @@ are reserved for internal uses. Name: " + name);
        }
    }

    private Value buildValue(Element valueEl) throws Exception {
        Element fieldEl = DocumentHelper.getElementChild(valueEl, "field", false);

        if (fieldEl != null) {
            String fieldId = parseQNameGetId(DocumentHelper.getAttribute(fieldEl, "name", true), fieldEl);
            return new FieldValue(fieldId);
        }

        Element derefEl = DocumentHelper.getElementChild(valueEl, "deref", false);
        if (derefEl != null) {
            Element[] children = DocumentHelper.getElementChildren(derefEl);

            Element lastEl = children[children.length - 1];

            if (!lastEl.getLocalName().equals("field") || lastEl.getNamespaceURI() != null) {
                throw new IndexerConfException("Last element in a <deref> should be a field, at " +
                        LocationAttributes.getLocationString(lastEl));
            }

            if (children.length == 1) {
                throw new IndexerConfException("A <deref> should contain one or more <follow> elements.");
            }

            String fieldId = parseQNameGetId(DocumentHelper.getAttribute(lastEl, "name", true), lastEl);

            DerefValue deref = new DerefValue(fieldId);

            // Run over all children except the last
            for (int i = 0; i < children.length - 1; i++) {
                Element child = children[i];
                if (child.getLocalName().equals("follow") || child.getNamespaceURI() == null) {
                    String field = DocumentHelper.getAttribute(child, "field", false);
                    String variant = DocumentHelper.getAttribute(child, "variant", false);
                    if (field != null) {
                        String followFieldId = parseQNameGetId(DocumentHelper.getAttribute(child, "field", true), child);
                        deref.addFieldFollow(followFieldId);
                    } else if (variant != null) {
                        if (variant.equals("master")) {
                            deref.addMasterFollow();
                        } else {
                            // The variant dimensions are specified in a syntax like "-var1,-var2,-var3"
                            boolean validConfig = true;
                            Set<String> dimensions = new HashSet<String>();
                            String[] ops = variant.split(",");
                            for (String op : ops) {
                                op = op.trim();
                                if (op.length() > 1 && op.startsWith("-")) {
                                    String dimension = op.substring(1);
                                    dimensions.add(dimension);
                                } else {
                                    validConfig = false;
                                    break;
                                }
                            }
                            if (dimensions.size() == 0)
                                validConfig = false;

                            if (!validConfig) {
                                throw new IndexerConfException("Invalid specification of variants to follow: \"" +
                                        variant + "\".");
                            }

                            deref.addVariantFollow(dimensions);
                        }
                    } else {
                        throw new IndexerConfException("Required attribute missing on <follow> at " +
                                LocationAttributes.getLocation(child));
                    }
                } else {
                    throw new IndexerConfException("Unexpected element <" + child.getTagName() + "> at " +
                            LocationAttributes.getLocation(child));
                }
            }

            return deref;
        }

        throw new RuntimeException("No value configured for index field at " + LocationAttributes.getLocation(valueEl));
    }

    private QName parseQName(String qname, Element contextEl) throws IndexerConfException {
        int colonPos = qname.indexOf(":");
        if (colonPos == -1) {
            throw new IndexerConfException("Field name is not a qualified name, it should include a namespace prefix: " + qname);
        }

        String prefix = qname.substring(0, colonPos);
        String localName = qname.substring(colonPos + 1);

        String uri = contextEl.lookupNamespaceURI(prefix);
        if (uri == null) {
            throw new IndexerConfException("Prefix does not resolve to a namespace: " + qname);
        }

        return new QName(uri, localName);
    }

    private String parseQNameGetId(String qname, Element contextEl) throws IndexerConfException {
        QName parsedQName = parseQName(qname, contextEl);

        try {
            return typeManager.getFieldTypeByName(parsedQName).getId();
        } catch (FieldTypeNotFoundException e) {
            throw new IndexerConfException("unknown field type: " + parsedQName, e);
        }
    }

}
