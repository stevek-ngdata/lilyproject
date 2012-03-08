package org.lilyproject.tools.import_.json;

import org.codehaus.jackson.*;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.map.*;
import org.codehaus.jackson.map.ser.StdSerializerProvider;
import org.codehaus.jackson.map.type.*;
import org.codehaus.jackson.type.JavaType;
import org.lilyproject.bytes.api.ByteArray;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.exception.ExceptionUtil;
import org.lilyproject.util.json.JsonFormat;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.List;

/**
 * Provides mapping from/to JSON of {@link RecordScan} objects and
 * {@link org.lilyproject.repository.api.filter.RecordFilter}'s.
 *
 * <p>It is a generic mapping, rather than needing custom logic for each kind of filter.
 * Custom logic actually has a lot of advantages, such as ultimate flexibility
 * (not being limited by what the generic framework can handle, such as doing
 * serialization of unforeseen data types), being able to detect
 * older serialization versions and apply necessary conversion, etc. After all,
 * since this serialization format is also used in the REST interface, it
 * is a contract towards our users, and it should also be a nice readable
 * format.
 *
 * <p>Looking at the implementation here, it was actually rather involved
 * to customize Jackson to get the desired serialization.
 *
 * <p>The reasons to go with it anyway are:</p>
 *
 * <ul>
 *     <li>It avoids to write this logic for each individual filter implementation,
 *     thus should be less error-prone and less divergent between implementations
 *
 *     <li>Since the filter definition objects are part of the core Lily API
 *     (for the built-in filters), adding from/toJson methods to them was not
 *     attractive for two reasons: it would make the API dependent on some json
 *     library (jackson), and for the serialization of field values (such as in
 *     FieldValueFilter), a complete (and extensible) serialization for each kind
 *     of Lily field value would need to be part of the API. This felt like moving
 *     a lot of impl code in the API.
 *
 *     <li>To avoid adding code to the API, we could have foreseen an extensible
 *     filter-json-mapping as part of the json conversion library, into which
 *     people providing new filter implementations could add their own implementations.
 *     This would then require an additional class per filter, which is feasible
 *     but causes lots of extra classes.
 * </ul>
 *
 * <p><b>Convention:</b> if you have a field in a filter which is declared as type Object,
 * it is assumed this field contains a Lily field value, and will be (de)serialized
 * as such (if ever this would be too restrictive, this could be based on an annotation
 * instead).</p>
 *
 * <p>Hidden in here, there is code to (de)serialize Lily field values without
 * needing access to the FieldType. Thus the (de)serializing of the field value
 * is done without knowing what field type and thus value type it is. This is
 * done by detecting the value type based on the type of the Java object(s).</p>
 */
public class RecordScanJsonMapper {
    protected static ThreadLocal<Repository> LOCAL_REPOSITORY = new ThreadLocal<Repository>();
    private ObjectMapper mapper;

    protected static RecordScanJsonMapper INSTANCE = new RecordScanJsonMapper();
    
    public RecordScanJsonMapper() {
        mapper = new ObjectMapper();

        // The module provides our custom (de)serializers
        mapper.registerModule(new ScanModule());

        // Jackson first determines the type in case of polymorphism, then calls the de-serializers.
        // In case of the RecordId, which is an interface, we serialize it as a simple string,
        // so there is no type information embedded in the json (the type information is actually
        // part of the RecordId string). To avoid Jackson complaining there is no type info in this
        // case, we use a custom TypeResolverBuilder.
        mapper.setDefaultTyping(new ScanDefaultTypeResolverBuilder(ObjectMapper.DefaultTyping.OBJECT_AND_NON_CONCRETE));

        // To use different serializers depending on the declared property type, rather than the actual
        // value type, I have not found any other way then using a custom SerializerProvider.
        // Thus: if you have a been containing "Object value", and value is a String, then by default
        // jackson will use the String to look up a serializer, rather than Object. 
        mapper.setSerializerProvider(new ScanSerializerProvider());

        // Be flexible towards our users
        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    }

    public ObjectMapper getMapper() {
        return mapper;
    }

    public static class ScanSerializerProvider extends StdSerializerProvider {
        public ScanSerializerProvider() {
            super();
        }

        protected ScanSerializerProvider(SerializationConfig config, StdSerializerProvider src, SerializerFactory f) {
            super(config, src, f);
        }

        @Override
        public JsonSerializer<Object> findValueSerializer(Class<?> valueType, BeanProperty property)
                throws JsonMappingException {
            // On serialization, Jackson selects the serializer based on the value rather than based on the declared
            // type of the instance variable. We consider any property typed as Object as being a Lily field value.
            // (the alternative would be to require some annotation to indicate this, which we could do at some point).
            if (safeGetClassFromProp(property) == Object.class) {
                return new FieldValueSerializer();
            }
            return super.findValueSerializer(valueType, property);
        }

        protected StdSerializerProvider createInstance(SerializationConfig config, SerializerFactory jsf) {
            return new ScanSerializerProvider(config, this, jsf);
        }
    }

    public static class ScanModule extends Module {
        @Override
        public String getModuleName() {
            return "LilyScan";
        }

        @Override
        public Version version() {
            return new Version(1, 0, 0, null);
        }

        @Override
        public void setupModule(SetupContext context) {
            context.addSerializers(new ScanSerializers());
            context.addDeserializers(new ScanDeserializers());
        }
    }

    public static class ScanDefaultTypeResolverBuilder extends ObjectMapper.DefaultTypeResolverBuilder {
        public ScanDefaultTypeResolverBuilder(ObjectMapper.DefaultTyping t) {
            super(t);
            // use @class style embedding of class info in case of polymorphic types
            super.init(JsonTypeInfo.Id.CLASS, null);
            super.inclusion(JsonTypeInfo.As.PROPERTY);
            super.typeProperty("@class");
        }

        @Override
        public boolean useForType(JavaType t) {
            if (t.getRawClass().equals(RecordId.class)) {
                // RecordId's are serialized as plain strings, don't use type resolving
                return false;
            } else if (t.getRawClass() == Object.class) {
                // On de-serialization, Object is the declared property class, field values
                return false;
            } else if (t.getRawClass().equals(List.class)) {
                // Don't add type info (like "java.util.ArrayList") for lists.
                return false;
            } else {
                return super.useForType(t);
            }
        }
    }

    private static Class safeGetClassFromProp(BeanProperty property) {
        return property != null && property.getType() != null ? property.getType().getRawClass() : null;
    }

    public static class ScanSerializers implements Serializers {
        @Override
        public JsonSerializer<?> findSerializer(SerializationConfig config, JavaType type, BeanDescription beanDesc,
                BeanProperty property) {
            if (RecordId.class.isAssignableFrom(type.getRawClass())) {
                return new RecordIdSerializer();
            } else if (QName.class.isAssignableFrom(type.getRawClass())) {
                return new QNameSerializer();
            } else if (Object.class == type.getRawClass()) {
                return new FieldValueSerializer();
            }
            return null;
        }

        @Override
        public JsonSerializer<?> findArraySerializer(SerializationConfig config, ArrayType type,
                BeanDescription beanDesc, BeanProperty property, TypeSerializer elementTypeSerializer,
                JsonSerializer<Object> elementValueSerializer) {
            return null;
        }

        @Override
        public JsonSerializer<?> findCollectionSerializer(SerializationConfig config, CollectionType type,
                BeanDescription beanDesc, BeanProperty property, TypeSerializer elementTypeSerializer,
                JsonSerializer<Object> elementValueSerializer) {
            return null;
        }

        @Override
        public JsonSerializer<?> findCollectionLikeSerializer(SerializationConfig config, CollectionLikeType type,
                BeanDescription beanDesc, BeanProperty property, TypeSerializer elementTypeSerializer,
                JsonSerializer<Object> elementValueSerializer) {
            return null;
        }

        @Override
        public JsonSerializer<?> findMapSerializer(SerializationConfig config, MapType type, BeanDescription beanDesc,
                BeanProperty property, JsonSerializer<Object> keySerializer, TypeSerializer elementTypeSerializer,
                JsonSerializer<Object> elementValueSerializer) {
            return null;
        }

        @Override
        public JsonSerializer<?> findMapLikeSerializer(SerializationConfig config, MapLikeType type,
                BeanDescription beanDesc, BeanProperty property, JsonSerializer<Object> keySerializer,
                TypeSerializer elementTypeSerializer, JsonSerializer<Object> elementValueSerializer) {
            return null;
        }
    }

    public static class ScanDeserializers implements Deserializers {
        @Override
        public JsonDeserializer<?> findBeanDeserializer(JavaType type, DeserializationConfig config,
                DeserializerProvider provider, BeanDescription beanDesc, BeanProperty property) throws JsonMappingException {
            
            if (RecordId.class.isAssignableFrom(type.getRawClass())) {
                return new RecordIdDeserializer();
            } else if (QName.class.isAssignableFrom(type.getRawClass())) {
                return new QNameDeserializer();
            } else if (Object.class == type.getRawClass()) {
                return new FieldValueDeserializer();
            } else if (Object.class == safeGetClassFromProp(property)) {
                return new FieldValueDeserializer();
            }
            
            return null;
        }

        @Override
        public JsonDeserializer<?> findArrayDeserializer(ArrayType type, DeserializationConfig config,
                DeserializerProvider provider, BeanProperty property, TypeDeserializer elementTypeDeserializer,
                JsonDeserializer<?> elementDeserializer) throws JsonMappingException {
            return null;
        }

        @Override
        public JsonDeserializer<?> findCollectionDeserializer(CollectionType type, DeserializationConfig config,
                DeserializerProvider provider, BeanDescription beanDesc, BeanProperty property,
                TypeDeserializer elementTypeDeserializer, JsonDeserializer<?> elementDeserializer)
                throws JsonMappingException {
            return null;
        }

        @Override
        public JsonDeserializer<?> findCollectionLikeDeserializer(CollectionLikeType type, DeserializationConfig config,
                DeserializerProvider provider, BeanDescription beanDesc, BeanProperty property,
                TypeDeserializer elementTypeDeserializer, JsonDeserializer<?> elementDeserializer)
                throws JsonMappingException {
            return null;
        }

        @Override
        public JsonDeserializer<?> findEnumDeserializer(Class<?> type, DeserializationConfig config,
                BeanDescription beanDesc, BeanProperty property) throws JsonMappingException {
            return null;
        }

        @Override
        public JsonDeserializer<?> findMapDeserializer(MapType type, DeserializationConfig config,
                DeserializerProvider provider, BeanDescription beanDesc, BeanProperty property,
                KeyDeserializer keyDeserializer, TypeDeserializer elementTypeDeserializer,
                JsonDeserializer<?> elementDeserializer) throws JsonMappingException {
            return null;
        }

        @Override
        public JsonDeserializer<?> findMapLikeDeserializer(MapLikeType type, DeserializationConfig config,
                DeserializerProvider provider, BeanDescription beanDesc, BeanProperty property,
                KeyDeserializer keyDeserializer, TypeDeserializer elementTypeDeserializer,
                JsonDeserializer<?> elementDeserializer) throws JsonMappingException {
            return null;
        }

        @Override
        public JsonDeserializer<?> findTreeNodeDeserializer(Class<? extends JsonNode> nodeType,
                DeserializationConfig config, BeanProperty property) throws JsonMappingException {
            return null;
        }

    }

    public byte[] serialize(Object object) throws IOException {
        return mapper.writeValueAsBytes(object);
    }

    public <T> T deserialize(byte[] data, Class<T> clazz) throws IOException {
        return mapper.readValue(data, clazz);
    }

    public static class QNameDeserializer extends JsonDeserializer<QName> {
        @Override
        public QName deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            return QName.fromString(jp.getText());
        }

        @Override
        public QName deserialize(JsonParser jp, DeserializationContext ctxt, QName intoValue)
                throws IOException {
            return super.deserialize(jp, ctxt, intoValue);
        }
    }
    
    public static class QNameSerializer extends JsonSerializer<QName> {
        @Override
        public void serialize(QName value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeString(value.toString());
        }
    }

    public static class RecordIdSerializer extends JsonSerializer<RecordId> {
        @Override
        public void serialize(RecordId recordId, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeString(recordId.toString());
        }
    }
    
    public static class RecordIdDeserializer extends JsonDeserializer<RecordId> {
        @Override
        public RecordId deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) 
                throws IOException {
            return LOCAL_REPOSITORY.get().getIdGenerator().fromString(jsonParser.getText());
        }
    }

    public static class FieldValueSerializer extends JsonSerializer<Object> {

        @Override
        public void serialize(Object value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            serializeFieldValue(value, jgen);
        }
    }

    public static class FieldValueDeserializer extends JsonDeserializer<Object> {
        @Override
        public Object deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            if (jp.getCurrentToken() != JsonToken.START_OBJECT) {
                throw new RuntimeException("Expected start object token");
            }
            
            String valueTypeName = null;
            JsonNode valueNode = null;

            while (jp.nextToken() != JsonToken.END_OBJECT) {
                // move to field value
                jp.nextToken();

                // read field value
                if (jp.getCurrentName().equals("valueType")) {
                    valueTypeName = jp.getText();
                } else if (jp.getCurrentName().equals("value")) {
                    // We have to use another ObjectMapper here, it seems that even in case you request
                    // a generic tree model, Jackson still uses some of the customizations we did through
                    // the Module
                    valueNode = JsonFormat.OBJECT_MAPPER.readTree(jp);
                } else {
                    throw new RuntimeException("Unexpected field in json representation of QName: " +
                            jp.getCurrentName());
                }
            }
            
            if (valueTypeName == null) {
                throw new RuntimeException("Missing valueType attribute for Lily field value.");
            }
            if (valueNode == null) {
                throw new RuntimeException("Missing value attribute for Lily field value.");
            }

            try {
                Repository repository = LOCAL_REPOSITORY.get();
                ValueType valueType = repository.getTypeManager().getValueType(valueTypeName);
                Object value = RecordReader.INSTANCE.readValue(valueNode, valueType, "value", new NamespacesImpl(), repository);
                return value;
            } catch (Exception e) {
                ExceptionUtil.handleInterrupt(e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Generates a Lily value type string for a given value. For LIST-type values, it assumes
     * the first value in the list is representative for all the values in the list, it is not
     * validated that all list entries are of the same type (this would be costly, it is
     * assumed this is check when serializing or de-serializing).
     */
    private static String determineValueType(Object value) {
        String typeName;
        
        if (value instanceof List) {
            List list = (List)value;
            if (list.size() == 0) {
                // the type doesn't matter, but is obliged, so just use string
                return "LIST<STRING>";
            } else {
                typeName = "LIST<" + determineValueType(list.get(0)) + ">";
            }
        } else if (value instanceof HierarchyPath) {
            HierarchyPath path = (HierarchyPath)value;
            if (path.size() == 0) {
                // the type doesn't matter, but is obliged, so just use string
                return "PATH<STRING>";
            } else {
                typeName = "PATH<" + determineValueType(path.get(0)) + ">";
            }
        } else if (value instanceof String) {
            typeName = "STRING";
        } else if (value instanceof Integer) {
            typeName = "INTEGER";
        } else if (value instanceof Long) {
            typeName = "LONG";
        } else if (value instanceof Double) {
            typeName = "DOUBLE";
        } else if (value instanceof BigDecimal) {
            typeName = "DECIMAL";
        } else if (value instanceof Boolean) {
            typeName = "BOOLEAN";
        } else if (value instanceof org.joda.time.LocalDate) {
            typeName = "DATE";
        } else if (value instanceof org.joda.time.DateTime) {
            typeName = "DATETIME";
        } else if (value instanceof Blob) {
            typeName = "BLOB";
        } else if (value instanceof Link) {
            typeName = "LINK";
        } else if (value instanceof URI) {
            typeName = "URI";
        } else if (value instanceof Record) {
            typeName = "RECORD";
        } else if (value instanceof ByteArray) {
            typeName = "BYTEARRAY";
        } else {
            throw new RuntimeException("This type of object is not supported by the JSON field value serialization: " +
                    value.getClass().getName());
        }
        
        return typeName;
    }

    private static void serializeFieldValue(Object value, JsonGenerator jgen) {
        try {
            Repository repository = LOCAL_REPOSITORY.get();
            String valueTypeName = determineValueType(value);
            ValueType valueType = repository.getTypeManager().getValueType(valueTypeName);
            WriteOptions options = new WriteOptions();
            Namespaces namespaces = new NamespacesImpl(false);
            
            JsonNode node = RecordWriter.INSTANCE.valueToJson(value, valueType, options, namespaces, repository);

            jgen.writeStartObject();
            jgen.writeStringField("valueType", valueTypeName);
            jgen.writeFieldName("value");
            jgen.writeTree(node);
            jgen.writeEndObject();
        } catch (Exception e) {
            ExceptionUtil.handleInterrupt(e);
            throw new RuntimeException("Error serializing field value.", e);
        }
    }
}
