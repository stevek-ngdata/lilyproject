package org.lilycms.indexer.conf;

import org.lilycms.util.xml.XmlProducer;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The configuration for the indexer, describes how record types should be mapped
 * onto index documents.
 */
public class IndexerConf {
    private Map<String, RecordTypeMapping> versionedContentMappings = new HashMap<String, RecordTypeMapping>();
    protected Map<String, IndexFieldType> fieldTypes = new HashMap<String, IndexFieldType>();
    protected Map<String, IndexField> fields = new HashMap<String, IndexField>();
    protected String defaultSearchField;

    public RecordTypeMapping getVersionedContentMapping(String recordTypeName, String versionTag) {
        return versionedContentMappings.get(getMappingKey(recordTypeName, versionTag));
    }

    protected void addVersionedContentMapping(String recordTypeName, String versionTag, RecordTypeMapping mapping) {
        versionedContentMappings.put(getMappingKey(recordTypeName, versionTag), mapping);
    }

    private String getMappingKey(String recordTypeName, String versionTag){
        return recordTypeName + "-" + versionTag;
    }

    public void generateSolrSchema(OutputStream os) throws Exception {
        XmlProducer serializer = new XmlProducer(os);

        Map<String, String> attrs = new LinkedHashMap<String, String>();
        attrs.put("name", "lily");
        attrs.put("version", "1.2");

        serializer.startElement("schema", attrs);

        //
        // Output types
        //
        serializer.startElement("types");

        for (IndexFieldType fieldType : fieldTypes.values()) {
            serializer.embedXml(fieldType.getDefinition());
        }

        // Built-in string type
        attrs.clear();
        attrs.put("name", "@@string");
        attrs.put("class", "solr.StrField");
        attrs.put("sortMissingLast", "true");
        attrs.put("omitNorms", "true");
        serializer.emptyElement("fieldType", attrs);

        serializer.endElement("types");

        //
        // Output fields
        //
        serializer.startElement("fields");

        for (IndexField field : fields.values()) {
            attrs.clear();
            attrs.put("name", field.getName());
            attrs.put("type", field.getType().getName());
            attrs.put("indexed", String.valueOf(field.getIndexed()));
            attrs.put("stored", String.valueOf(field.getStored()));
            // TODO the remainder of the attributes
            serializer.emptyElement("field", attrs);
        }

        // Built-in @@id field
        attrs.clear();
        attrs.put("name", "@@id");
        attrs.put("type", "@@string");
        attrs.put("indexed", "true");
        attrs.put("stored", "true");
        attrs.put("required", "true");
        serializer.emptyElement("field", attrs);

        serializer.endElement("fields");

        //
        // Other
        //
        serializer.simpleElement("uniqueKey", "@@id");

        if (defaultSearchField != null) {
            serializer.simpleElement("defaultSearchField", defaultSearchField);
        }

        serializer.endElement("schema");

        serializer.flush();
    }
}
