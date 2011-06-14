/*
 * Copyright 2010 Outerthought bvba
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
package org.lilyproject.indexer.model.indexerconf;

import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.regex.Pattern;

import javax.xml.XMLConstants;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.lilyproject.repository.api.*;
import org.lilyproject.util.location.LocationAttributes;
import org.lilyproject.util.repo.SystemFields;
import org.lilyproject.util.repo.VersionTag;
import org.lilyproject.util.xml.DocumentHelper;
import org.lilyproject.util.xml.LocalXPathExpression;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

// Terminology: the word "field" is usually used for a field from a repository record, while
// the term "index field" is usually used for a field in the index, though sometimes these
// are also just called field.
public class IndexerConfBuilder {
    private static LocalXPathExpression INDEX_CASES =
            new LocalXPathExpression("/indexer/records/record");

    private static LocalXPathExpression FORMATTERS =
            new LocalXPathExpression("/indexer/formatters/formatter");

    private static LocalXPathExpression INDEX_FIELDS =
            new LocalXPathExpression("/indexer/fields/field");

    private static LocalXPathExpression DYNAMIC_INDEX_FIELDS =
            new LocalXPathExpression("/indexer/dynamicFields/dynamicField");

    private Document doc;

    private IndexerConf conf;

    private Repository repository;

    private TypeManager typeManager;

    private SystemFields systemFields;

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
        validate(doc);
        this.doc = doc;
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
        this.systemFields = SystemFields.getInstance(repository.getTypeManager(), repository.getIdGenerator());
        this.conf = new IndexerConf();
        this.conf.setSystemFields(systemFields);

        try {
            buildCases();
            buildFormatters();
            buildIndexFields();
            buildDynamicFields();
        } catch (Exception e) {
            throw new IndexerConfException("Error in the configuration.", e);
        }

        return conf;
    }

    private void buildCases() throws Exception {
        List<Element> cases = INDEX_CASES.get().evalAsNativeElementList(doc);
        for (Element caseEl : cases) {
            WildcardPattern matchNamespace = null;
            WildcardPattern matchName = null;

            String matchNamespaceAttr = DocumentHelper.getAttribute(caseEl, "matchNamespace", false);

            if (matchNamespaceAttr != null) {
                // If the matchNamespace attr does not contain a wildcard expression, and its value
                // happens to be an existing namespace prefix, than substitute the prefix for the full URI.
                if (!WildcardPattern.isWildcardExpression(matchNamespaceAttr)) {
                    String uri = caseEl.lookupNamespaceURI(matchNamespaceAttr);
                    if (uri != null)
                        matchNamespaceAttr = uri;
                }
                matchNamespace = new WildcardPattern(matchNamespaceAttr);
            }

            String matchNameAttr = DocumentHelper.getAttribute(caseEl, "matchName", false);

            if (matchNameAttr != null) {
                matchName = new WildcardPattern(matchNameAttr);
            }

            String vtagsSpec = DocumentHelper.getAttribute(caseEl, "vtags", false);

            Map<String, String> varPropsPattern = parseVariantPropertiesPattern(caseEl);
            Set<SchemaId> vtags = parseVersionTags(vtagsSpec);

            IndexCase indexCase = new IndexCase(matchNamespace, matchName, varPropsPattern, vtags);
            conf.addIndexCase(indexCase);
        }
    }

    private void buildFormatters() throws Exception {
        List<Element> formatters = FORMATTERS.get().evalAsNativeElementList(doc);
        for (Element formatterEl : formatters) {
            String className = DocumentHelper.getAttribute(formatterEl, "class", true);
            Formatter formatter = instantiateFormatter(className);

            String name = DocumentHelper.getAttribute(formatterEl, "name", false);
            String type = DocumentHelper.getAttribute(formatterEl, "type", false);

            Set<String> types;
            if (type == null) {
                types = formatter.getSupportedPrimitiveValueTypes();
            } else if (type.trim().equals("*")) {
                types = Collections.emptySet();
            } else {
                // Check the specified types are a subset of those supported by the formatter
                types = new HashSet<String>();
                Set<String> supportedTypes = formatter.getSupportedPrimitiveValueTypes();
                List<String> specifiedTypes = parseCSV(type);
                for (String item : specifiedTypes) {
                    if (supportedTypes.contains(item)) {
                        types.add(item);
                    } else {
                        throw new IndexerConfException("Formatter definition error: primitive value type "
                                + item + " is not supported by formatter " + className);
                    }
                }
            }

            boolean singleValue = DocumentHelper.getBooleanAttribute(formatterEl, "singleValue", formatter.supportsSingleValue());
            boolean multiValue = DocumentHelper.getBooleanAttribute(formatterEl, "multiValue", formatter.supportsMultiValue());
            boolean nonHierarchical = DocumentHelper.getBooleanAttribute(formatterEl, "nonHierarchical", formatter.supportsNonHierarchicalValue());
            boolean hierarchical = DocumentHelper.getBooleanAttribute(formatterEl, "hierarchical", formatter.supportsHierarchicalValue());

            String message = "Formatter does not support %1$s. Class " + className + " at " + LocationAttributes.getLocation(formatterEl);

            if (singleValue && !formatter.supportsSingleValue())
                throw new IndexerConfException(String.format(message, "singleValue"));
            if (multiValue && !formatter.supportsMultiValue())
                throw new IndexerConfException(String.format(message, "multiValue"));
            if (hierarchical && !formatter.supportsHierarchicalValue())
                throw new IndexerConfException(String.format(message, "hierarchical"));
            if (nonHierarchical && !formatter.supportsNonHierarchicalValue())
                throw new IndexerConfException(String.format(message, "nonHierarchical"));

            if (name != null && conf.getFormatters().hasFormatter(name)) {
                throw new IndexerConfException("Duplicate formatter name: " + name);
            }

            conf.getFormatters().addFormatter(formatter, name, types, singleValue, multiValue, nonHierarchical, hierarchical);
        }
    }

    private Formatter instantiateFormatter(String className) throws IndexerConfException {
        ClassLoader contextCL = Thread.currentThread().getContextClassLoader();
        Class formatterClass;
        try {
            formatterClass = contextCL.loadClass(className);
        } catch (ClassNotFoundException e) {
            throw new IndexerConfException("Error loading formatter class " + className + " from context class loader.", e);
        }

        if (!Formatter.class.isAssignableFrom(formatterClass)) {
            throw new IndexerConfException("Specified formatter class does not implement Formatter interface: " + className);
        }

        try {
            return (Formatter)formatterClass.newInstance();
        } catch (Exception e) {
            throw new IndexerConfException("Error instantiating formatter class " + className, e);
        }
    }

    private List<String> parseCSV(String csv) {
        String[] parts = csv.split(",");

        List<String> result = new ArrayList<String>(parts.length);

        for (String part : parts) {
            part = part.trim();
            if (part.length() > 0)
                result.add(part);
        }

        return result;
    }

    private Map<String, String> parseVariantPropertiesPattern(Element caseEl) throws Exception {
        String variant = DocumentHelper.getAttribute(caseEl, "matchVariant", false);

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
                        throw new IndexerConfException(String.format("Error in matchVariant attribute: the character '*' " +
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

    private Set<SchemaId> parseVersionTags(String vtagsSpec) throws IndexerConfException, InterruptedException {
        Set<SchemaId> vtags = new HashSet<SchemaId>();

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
                } catch (RepositoryException e) {
                    throw new IndexerConfException("error loading field type for vtag: " + tag, e);
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

            Value value = buildValue(indexFieldEl);

            IndexField indexField = new IndexField(name, value);
            conf.addIndexField(indexField);
        }
    }

    private void buildDynamicFields() throws Exception {
        List<Element> fields = DYNAMIC_INDEX_FIELDS.get().evalAsNativeElementList(doc);
        for (Element fieldEl : fields) {
            String matchNamespaceAttr = DocumentHelper.getAttribute(fieldEl, "matchNamespace", false);
            String matchNameAttr = DocumentHelper.getAttribute(fieldEl, "matchName", false);
            String matchTypeAttr = DocumentHelper.getAttribute(fieldEl, "matchType", false);
            Boolean matchMultiValue = DocumentHelper.getBooleanAttribute(fieldEl, "matchMultiValue", null);
            Boolean matchHierarchical = DocumentHelper.getBooleanAttribute(fieldEl, "matchHierarchical", null);
            String matchScopeAttr = DocumentHelper.getAttribute(fieldEl, "matchScope", false);
            String nameAttr = DocumentHelper.getAttribute(fieldEl, "name", true);

            WildcardPattern matchNamespace = null;
            if (matchNamespaceAttr != null) {
                // If the matchNamespace attr does not contain a wildcard expression, and its value
                // happens to be an existing namespace prefix, than substitute the prefix for the full URI.
                if (!WildcardPattern.isWildcardExpression(matchNamespaceAttr)) {
                    String uri = fieldEl.lookupNamespaceURI(matchNamespaceAttr);
                    if (uri != null)
                        matchNamespaceAttr = uri;
                }
                matchNamespace = new WildcardPattern(matchNamespaceAttr);
            }

            WildcardPattern matchName = null;
            if (matchNameAttr != null) {
                matchName = new WildcardPattern(matchNameAttr);
            }

            Set<String> matchTypes = null;
            if (matchTypeAttr != null) {
                matchTypes = new HashSet<String>();
                String[] types = matchTypeAttr.split(",");
                for (String type : types) {
                    matchTypes.add(type.toUpperCase());
                }
                if (matchTypes.isEmpty()) {
                    matchTypes = null;
                }
            }

            Set<Scope> matchScopes = null;
            if (matchScopeAttr != null) {
                matchScopes = EnumSet.noneOf(Scope.class);
                String[] scopes = matchScopeAttr.split(",");
                for (String scope : scopes) {
                    matchScopes.add(Scope.valueOf(scope));
                }
                if (matchScopes.isEmpty()) {
                    matchScopes = null;
                }
            }

            Set<String> variables = new HashSet<String>();
            variables.add("namespace");
            variables.add("name");
            variables.add("primitiveType");
            variables.add("primitiveTypeLC");
            variables.add("multiValue");
            variables.add("hierarchical");
            if (matchName != null && matchName.hasWildcard())
                variables.add("nameMatch");
            if (matchNamespace != null && matchNamespace.hasWildcard())
                variables.add("namespaceMatch");

            Set<String> booleanVariables = new HashSet<String>();
            booleanVariables.add("multiValue");
            booleanVariables.add("hierarchical");

            NameTemplate name = new NameTemplate(nameAttr, variables, booleanVariables);

            boolean extractContent = DocumentHelper.getBooleanAttribute(fieldEl, "extractContent", false);
            String formatter = DocumentHelper.getAttribute(fieldEl, "formatter", false);
            if (formatter != null && !conf.getFormatters().hasFormatter(formatter)) {
                throw new IndexerConfException("Formatter does not exist: " + formatter + " at " +
                        LocationAttributes.getLocationString(fieldEl));
            }

            DynamicIndexField field = new DynamicIndexField(matchNamespace, matchName, matchTypes, matchMultiValue,
                    matchHierarchical, matchScopes, name, extractContent, formatter);

            conf.addDynamicIndexField(field);
        }
    }

    private void validateName(String name) throws IndexerConfException {
        if (name.startsWith("lily.")) {
            throw new IndexerConfException("names starting with 'lily.' are reserved for internal uses. Name: " + name);
        }
    }

    private Value buildValue(Element fieldEl) throws Exception {
        String valueExpr = DocumentHelper.getAttribute(fieldEl, "value", true);

        FieldType fieldType;
        Value value;

        boolean extractContent = DocumentHelper.getBooleanAttribute(fieldEl, "extractContent", false);

        String formatter = DocumentHelper.getAttribute(fieldEl, "formatter", false);
        if (formatter != null && !conf.getFormatters().hasFormatter(formatter)) {
            throw new IndexerConfException("Formatter does not exist: " + formatter + " at " +
                    LocationAttributes.getLocationString(fieldEl));
        }

        //
        // An index field can basically map to two kinds of values:
        //   * plain field values
        //   * dereference expressions (following links to some other record and then taking a field value from it)
        //

        // A derefence expression is specified as "somelink=>somelink=>somefield"

        if (valueExpr.contains("=>")) {
            //
            // Split, normalize, validate the input
            //
            String[] derefParts = valueExpr.split(Pattern.quote("=>"));
            for (int i = 0; i < derefParts.length; i++) {
                String trimmed = derefParts[i].trim();
                if (trimmed.length() == 0) {
                    throw new IndexerConfException("Invalid dereference expression '" + valueExpr + "' at "
                        + LocationAttributes.getLocationString(fieldEl));
                }
                derefParts[i] = trimmed;
            }

            if (derefParts.length < 2) {
                throw new IndexerConfException("Invalid dereference expression '" + valueExpr + "' at "
                    + LocationAttributes.getLocationString(fieldEl));
            }

            //
            // Last element in the list should be a field
            //
            QName targetFieldName;
            try {
                targetFieldName = parseQName(derefParts[derefParts.length - 1], fieldEl);
            } catch (IndexerConfException e) {
                throw new IndexerConfException("Dereference expression does not end on a valid field name. " +
                    "Expression: '" + valueExpr + "' at " + LocationAttributes.getLocationString(fieldEl), e);
            }
            fieldType = getFieldType(targetFieldName);

            DerefValue deref = new DerefValue(fieldType, extractContent, formatter);

            //
            // Run over all children except the last
            //
            for (int i = 0; i < derefParts.length - 1; i++) {
                String derefPart = derefParts[i];

                // A deref expression can contain 3 kinds of links:
                //  - link stored in a link field (detected based on presence of a colon)
                //  - link to the master variant (if it's the literal string 'master')
                //  - link to a less-dimensioned variant (all other cases)

                if (derefPart.contains(":")) { // Link field
                    FieldType followField = getFieldType(derefPart, fieldEl);
                    if (!followField.getValueType().getPrimitive().getName().equals("LINK")) {
                        throw new IndexerConfException("A non-link field is used in a dereference expression. " +
                                "Field: '" + derefPart + "', deref expression '" + valueExpr + "' " +
                                "at " + LocationAttributes.getLocation(fieldEl));
                    }
                    if (followField.getValueType().isHierarchical()) {
                        throw new IndexerConfException("A hierarchical link field is used in a dereference " +
                                "expression. Field: '" + derefPart + "', deref expression '" + valueExpr + "' " +
                                "at " + LocationAttributes.getLocation(fieldEl));
                    }
                    deref.addFieldFollow(followField);
                } else if (derefPart.equals("master")) { // Link to master variant
                    deref.addMasterFollow();
                } else {  // Link to less dimensioned variant
                    // The variant dimensions are specified in a syntax like "-var1,-var2,-var3"
                    boolean validConfig = true;
                    Set<String> dimensions = new HashSet<String>();
                    String[] ops = derefPart.split(",");
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
                        throw new IndexerConfException("Invalid specification of variants to follow: '" +
                                derefPart + "', deref expression: '" + valueExpr + "' " +
                                "at " + LocationAttributes.getLocation(fieldEl));
                    }

                    deref.addVariantFollow(dimensions);
                }
            }

            deref.init(typeManager);
            value = deref;
        } else {
            //
            // A plain field
            //
            fieldType = getFieldType(valueExpr, fieldEl);
            value = new FieldValue(fieldType, extractContent, formatter);
        }

        if (extractContent && !fieldType.getValueType().getPrimitive().getName().equals("BLOB")) {
            throw new IndexerConfException("extractContent is used for a non-blob value at "
                    + LocationAttributes.getLocation(fieldEl));
        }

        return value;
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

    private FieldType getFieldType(String qname, Element contextEl) throws IndexerConfException, InterruptedException,
            RepositoryException {
        QName parsedQName = parseQName(qname, contextEl);
        return getFieldType(parsedQName);
    }

    private FieldType getFieldType(QName qname) throws IndexerConfException, InterruptedException,
            RepositoryException {

        if (systemFields.isSystemField(qname)) {
            return systemFields.get(qname);
        }

        return typeManager.getFieldTypeByName(qname);
    }

    private void validate(Document document) throws IndexerConfException {
        try {
            SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            URL url = getClass().getClassLoader().getResource("org/lilyproject/indexer/model/indexerconf/indexerconf.xsd");
            Schema schema = factory.newSchema(url);
            Validator validator = schema.newValidator();
            validator.validate(new DOMSource(document));
        } catch (Exception e) {
            throw new IndexerConfException("Error validating indexer configuration against XML Schema.", e);
        }
    }

    public static void validate(InputStream is) throws IndexerConfException {
        MyErrorHandler errorHandler = new MyErrorHandler();

        try {
            SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            URL url = IndexerConfBuilder.class.getClassLoader().getResource("org/lilyproject/indexer/model/indexerconf/indexerconf.xsd");
            Schema schema = factory.newSchema(url);
            Validator validator = schema.newValidator();
            validator.setErrorHandler(errorHandler);
            validator.validate(new StreamSource(is));
        } catch (Exception e) {
            if (!errorHandler.hasErrors()) {
                throw new IndexerConfException("Error validating indexer configuration.", e);
            } // else it will be reported below
        }

        if (errorHandler.hasErrors()) {
            throw new IndexerConfException("The following errors occurred validating the indexer configuration:\n" +
                errorHandler.getMessage());
        }
    }

    private static class MyErrorHandler implements ErrorHandler {
        private StringBuilder builder = new StringBuilder();

        public void warning(SAXParseException exception) throws SAXException {
        }

        public void error(SAXParseException exception) throws SAXException {
            addException(exception);
        }

        public void fatalError(SAXParseException exception) throws SAXException {
            addException(exception);
        }

        public boolean hasErrors() {
            return builder.length() > 0;
        }

        public String getMessage() {
            return builder.toString();
        }

        private void addException(SAXParseException exception) {
            if (builder.length() > 0)
                builder.append("\n");

            builder.append("[").append(exception.getLineNumber()).append(":").append(exception.getColumnNumber());
            builder.append("] ").append(exception.getMessage());
        }
    }
}
