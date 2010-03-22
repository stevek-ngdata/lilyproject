package org.lilycms.indexer.conf;

import org.lilycms.util.location.LocationAttributes;
import org.lilycms.util.xml.DocumentHelper;
import org.lilycms.util.xml.LocalXPathExpression;
import org.lilycms.util.xml.XPathUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IndexerConfBuilder {
    private static LocalXPathExpression FIELDTYPES =
            new LocalXPathExpression("/indexer/mapping/fieldTypes/fieldType");

    private static LocalXPathExpression VERSIONED_MAPPING_CASES =
            new LocalXPathExpression("/indexer/mapping/versionedContentMapping/case");

    private static LocalXPathExpression NONVERSIONED_MAPPING_CASES =
            new LocalXPathExpression("/indexer/mapping/nonVersionedContentMapping/case");

    private static LocalXPathExpression FIELDS =
            new LocalXPathExpression("/indexer/mapping/*/case/indexField");

    private static LocalXPathExpression GLOBAL_FIELDS =
            new LocalXPathExpression("/indexer/mapping/globalFields/indexField");

    private static LocalXPathExpression FIELD_CHILDREN =
            new LocalXPathExpression("indexField");

    private Document doc;

    private IndexerConf conf;

    private IndexerConfBuilder() {
        // prevents instantiation
    }

    public static IndexerConf build(InputStream is) throws IndexerConfException {
        Document doc;
        try {
            doc = DocumentHelper.parse(is);
        } catch (Exception e) {
            throw new IndexerConfException("Error parsing supplied indexer configuration.", e);
        }
        return new IndexerConfBuilder().build(doc);
    }

    private IndexerConf build(Document doc) throws IndexerConfException {
        this.doc = doc;
        this.conf = new IndexerConf();

        try {
            buildFieldTypes();
            buildGlobalFields();
            buildFields();
            buildMappings();
            buildDefaultSearchField();
        } catch (Exception e) {
            throw new IndexerConfException("Error in the indexer configuration.", e);
        }

        return conf;
    }

    private void buildFieldTypes() throws Exception {
        List<Element> fieldTypes = FIELDTYPES.get().evalAsNativeElementList(doc);
        for (Element fieldType : fieldTypes) {
            String name = DocumentHelper.getAttribute(fieldType, "name", true);
            validateName(name);
            String className = DocumentHelper.getAttribute(fieldType, "class", true);
            if (conf.fieldTypes.containsKey(name)) {
                throw new IndexerConfException("Duplicate field type name " + name + " at " + LocationAttributes.getLocation(fieldType));
            }
            conf.fieldTypes.put(name, new IndexFieldType(name, className, fieldType));
        }
    }

    private void buildGlobalFields() throws Exception {
        List<Element> fields = FIELD_CHILDREN.get().evalAsNativeElementList(doc);
        for (Element field : fields) {
            String name = DocumentHelper.getAttribute(field, "name", false);
            validateName(name);
            if (name != null) {
                if (conf.fields.containsKey(name)) {
                    throw new IndexerConfException("Duplicate field name " + name + " at " + LocationAttributes.getLocation(field));
                }
                IndexField indexField = buildIndexField(name, field);
                conf.fields.put(indexField.getName(), indexField);
            }
        }
    }

    private void validateName(String name) throws IndexerConfException {
        if (name.startsWith("@@")) {
            throw new IndexerConfException("Indexer configuration: names starting with @@ are reserved for internal uses. Name: " + name);
        }
    }

    private IndexField buildIndexField(String name, Element field) throws Exception {
        String typeName = DocumentHelper.getAttribute(field, "type", true);
        IndexFieldType type = conf.fieldTypes.get(typeName);
        if (type == null) {
            throw new IndexerConfException("Reference to undefined type " + typeName + " at " + LocationAttributes.getLocation(field));
        }

        // !!! TODO read other properties like indexed, stored, etc.

        return new IndexField(name, type, true, true, false, false, false, false, false, null);
    }

    private void buildFields() throws Exception {
        List<Element> fields = FIELDS.get().evalAsNativeElementList(doc);
        for (Element field : fields) {
            String name = DocumentHelper.getAttribute(field, "name", false);
            validateName(name);
            // Name is optional because it can also be a ref to an elsewhere defined field
            if (name != null) {
                String qname = qualifyIndexFieldName(field);
                if (conf.fields.containsKey(qname)) {
                    throw new IndexerConfException("Duplicate field name " + name + " at " + LocationAttributes.getLocation(field));
                }
                IndexField indexField = buildIndexField(qname, field);
                conf.fields.put(indexField.getName(), indexField);
            }
        }
    }

    private String qualifyIndexFieldName(Element indexFieldEl) throws Exception {
        String name = DocumentHelper.getAttribute(indexFieldEl, "name", false);
        String recordTypeName = DocumentHelper.getAttribute((Element)indexFieldEl.getParentNode(), "recordType", true);
        return recordTypeName + "." + name;
    }

    private void buildMappings() throws Exception {
        List<Element> cases = VERSIONED_MAPPING_CASES.get().evalAsNativeElementList(doc);
        for (Element caseEl : cases) {
            String recordType = DocumentHelper.getAttribute(caseEl, "recordType", true);
            Set<String> versionTags = parseCSV(DocumentHelper.getAttribute(caseEl, "versionTags", true));

            // Check for duplicate mappings
            for (String versionTag : versionTags) {
                if (conf.getVersionedContentMapping(recordType, versionTag) != null) {
                    throw new IndexerConfException(String.format("Duplicate versioned content mapping for record type" +
                            " %1$s and version tag %2$s at %3$s", recordType, versionTag,
                            LocationAttributes.getLocation(caseEl)));
                }
            }

            RecordTypeMapping mapping = new RecordTypeMapping(recordType, versionTags);

            List<Element> indexFieldEls = FIELD_CHILDREN.get().evalAsNativeElementList(caseEl);
            for (Element indexFieldEl : indexFieldEls) {
                mapping.indexFieldBindings.add(buildIndexFieldMapping(indexFieldEl));
            }

            for (String versionTag : versionTags) {
                conf.addVersionedContentMapping(recordType, versionTag, mapping);
            }
        }
    }

    private IndexFieldBinding buildIndexFieldMapping(Element indexFieldEl) throws Exception {
        String name = DocumentHelper.getAttribute(indexFieldEl, "name", false);
        String ref = DocumentHelper.getAttribute(indexFieldEl, "ref", false);

        if ((name == null && ref == null) || (name != null && ref != null)) {
            throw new IndexerConfException(String.format("An indexField should have a name or a ref attribute, at %1$s",
                    LocationAttributes.getLocation(indexFieldEl)));
        }

        if (name != null) {
            validateName(name);
        }

        IndexField field;
        if (name != null) {
            field = conf.fields.get(qualifyIndexFieldName(indexFieldEl));
        } else {
            field = conf.fields.get(ref);
        }

        Value value = buildValue(DocumentHelper.getElementChild(indexFieldEl, "value", true));

        return new IndexFieldBinding(field, value);
    }

    private Value buildValue(Element valueEl) throws Exception {
        Element fieldEl = DocumentHelper.getElementChild(valueEl, "field", true);
        String name = DocumentHelper.getAttribute(fieldEl, "name", true);
        return new Value(name);
    }

    private Set<String> parseCSV(String input) {
        String[] values = input.split(",");
        Set<String> result = new HashSet<String>();
        for (String value : values) {
            value = value.trim();
            if (value.length() > 0) {
                result.add(value);
            }
        }
        return result;
    }

    private void validate(Document doc) {
        // TODO do basic structural validation using a schema / javax.xml.validation
        // TODO verify the uniqueness of non-global field names
        // TODO verify that all used global field names are defined

        // TODO? verify record types exist
        // TODO? verify record type compatibility
    }

    private void buildDefaultSearchField() {
        String defaultSearchField = XPathUtils.evalString("indexer/defaultSearchField", doc);
        if (defaultSearchField.length() > 0)
            conf.defaultSearchField = defaultSearchField;
    }
}
