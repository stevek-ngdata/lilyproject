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
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.XMLConstants;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeNotFoundException;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.util.location.LocationAttributes;
import org.lilyproject.util.repo.FieldValueStringConverter;
import org.lilyproject.util.repo.SystemFields;
import org.lilyproject.util.repo.VersionTag;
import org.lilyproject.util.xml.DocumentHelper;
import org.lilyproject.util.xml.LocalXPathExpression;
import org.lilyproject.util.xml.XPathUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

// Terminology: the word "field" is usually used for a field from a repositoryManager record, while
// the term "index field" is usually used for a field in the index, though sometimes these
// are also just called field.
public class IndexerConfBuilder {
    private static LocalXPathExpression INDEX_CASES =
            new LocalXPathExpression("/indexer/records/record");

    private static LocalXPathExpression RECORD_INCLUDE_FILTERS =
            new LocalXPathExpression("/indexer/recordFilter/includes/include");

    private static LocalXPathExpression RECORD_EXCLUDE_FILTERS =
            new LocalXPathExpression("/indexer/recordFilter/excludes/exclude");

    private static LocalXPathExpression FORMATTERS =
            new LocalXPathExpression("/indexer/formatters/formatter");

    private static LocalXPathExpression INDEX_FIELDS =
            new LocalXPathExpression("/indexer/fields");

    private static LocalXPathExpression DYNAMIC_INDEX_FIELDS =
            new LocalXPathExpression("/indexer/dynamicFields/dynamicField");

    private static final Splitter COMMA_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private static final Splitter EQUAL_SIGN_SPLITTER = Splitter.on('=').trimResults().omitEmptyStrings();

    private static final Splitter DEREF_SIGN_SPLITTER = Splitter.on("=>").trimResults().omitEmptyStrings();

    private final Log log = LogFactory.getLog(getClass());

    private Document doc;

    private IndexerConf conf;

    private RepositoryManager repositoryManager;

    private TypeManager typeManager;

    private SystemFields systemFields;

    private IndexerConfBuilder() {
        // prevents instantiation
    }

    public static IndexerConf build(InputStream is, RepositoryManager repositoryManager) throws IndexerConfException {
        Document doc;
        try {
            doc = DocumentHelper.parse(is);
        } catch (Exception e) {
            throw new IndexerConfException("Error parsing supplied configuration.", e);
        }
        return new IndexerConfBuilder().build(doc, repositoryManager);
    }

    private IndexerConf build(Document doc, RepositoryManager repositoryManager) throws IndexerConfException {
        validate(doc);
        this.doc = doc;
        this.repositoryManager = repositoryManager;
        this.typeManager = repositoryManager.getTypeManager();
        this.systemFields = SystemFields.getInstance(repositoryManager.getTypeManager(), repositoryManager.getIdGenerator());
        this.conf = new IndexerConf();
        this.conf.setSystemFields(systemFields);

        try {
            buildRecordFilter();
            buildFormatters();
            buildIndexFields();
            buildDynamicFields();
        } catch (Exception e) {
            throw new IndexerConfException("Error in the configuration.", e);
        }

        return conf;
    }

    private void buildRecordFilter() throws Exception {
        IndexRecordFilter recordFilter = new IndexRecordFilter();

        List<Element> includes = RECORD_INCLUDE_FILTERS.get().evalAsNativeElementList(doc);
        for (Element includeEl : includes) {
            RecordMatcher recordMatcher = parseRecordMatcher(includeEl);
            String vtagsSpec = DocumentHelper.getAttribute(includeEl, "vtags", true);
            Set<SchemaId> vtags = parseVersionTags(vtagsSpec);
            recordFilter.addInclude(recordMatcher, new IndexCase(vtags));
        }

        List<Element> excludes = RECORD_EXCLUDE_FILTERS.get().evalAsNativeElementList(doc);
        for (Element excludeEl : excludes) {
            RecordMatcher recordMatcher = parseRecordMatcher(excludeEl);
            recordFilter.addExclude(recordMatcher);
        }

        // This is for backwards compatibility: previously, <recordFilter> was called <records> and didn't have
        // excludes. This syntax was deprecated in 2.0.
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

            Map<String, String> varPropsPattern = parseVariantPropertiesPattern(caseEl, "matchVariant");
            Set<SchemaId> vtags = parseVersionTags(vtagsSpec);

            RecordMatcher recordMatcher = new RecordMatcher(matchNamespace, matchName, null, null, null, null,
                    varPropsPattern, typeManager);
            recordFilter.addInclude(recordMatcher, new IndexCase(vtags));
        }

        conf.setRecordFilter(recordFilter);
    }

    private RecordMatcher parseRecordMatcher(Element element) throws Exception {
        //
        // Condition on record type
        //
        WildcardPattern rtNamespacePattern = null;
        WildcardPattern rtNamePattern = null;

        String recordTypeAttr = DocumentHelper.getAttribute(element, "recordType", false);
        if (recordTypeAttr != null) {
            QName rtName = ConfUtil.parseQName(recordTypeAttr, element, true);
            rtNamespacePattern = new WildcardPattern(rtName.getNamespace());
            rtNamePattern = new WildcardPattern(rtName.getName());
        }

        //
        // "Instance of" condition
        //
        String instanceOfAttr = DocumentHelper.getAttribute(element, "instanceOf", false);
        QName instanceOfType = null;
        if (instanceOfAttr != null) {
            instanceOfType = ConfUtil.parseQName(instanceOfAttr, element, false);
        }

        //
        // Condition on variant properties
        //
        Map<String, String> varPropsPattern = parseVariantPropertiesPattern(element, "variant");

        //
        // Condition on field
        //
        String fieldAttr = DocumentHelper.getAttribute(element, "field", false);
        FieldType fieldType = null;
        RecordMatcher.FieldComparator comparator = null;
        Object fieldValue = null;
        if (fieldAttr != null) {
            int eqPos = fieldAttr.indexOf('='); // we assume = is not a symbol occurring in the field name
            if (eqPos == -1) {
                throw new IndexerConfException("field test should be of the form \"namespace:name(=|!=)value\", which " +
                        "the following is not: " + fieldAttr + ", at " + LocationAttributes.getLocation(element));
            }

            // not-equals support (simplistic parsing approach, doesn't need anything more complex for now)
            String namePart = fieldAttr.substring(0, eqPos);
            if (namePart.endsWith("!")) {
                namePart = namePart.substring(0, namePart.length() - 1);
                comparator = RecordMatcher.FieldComparator.NOT_EQUAL;
            } else {
                comparator = RecordMatcher.FieldComparator.EQUAL;
            }

            QName fieldName = ConfUtil.parseQName(namePart, element);
            fieldType = typeManager.getFieldTypeByName(fieldName);
            String fieldValueString = fieldAttr.substring(eqPos + 1);
            try {
                fieldValue = FieldValueStringConverter.fromString(fieldValueString, fieldType.getValueType(),
                        repositoryManager.getIdGenerator());
            } catch (IllegalArgumentException e) {
                throw new IndexerConfException("Invalid field value: " + fieldValueString);
            }
        }

        RecordMatcher matcher = new RecordMatcher(rtNamespacePattern, rtNamePattern, instanceOfType, fieldType,
                comparator, fieldValue, varPropsPattern, typeManager);

        return matcher;
    }

    private void buildFormatters() throws Exception {
        List<Element> formatters = FORMATTERS.get().evalAsNativeElementList(doc);
        for (Element formatterEl : formatters) {
            String className = DocumentHelper.getAttribute(formatterEl, "class", true);
            Formatter formatter = instantiateFormatter(className);

            String name = DocumentHelper.getAttribute(formatterEl, "name", true);

            if (name != null && conf.getFormatters().hasFormatter(name)) {
                throw new IndexerConfException("Duplicate formatter name: " + name);
            }

            conf.getFormatters().addFormatter(formatter, name);
        }

        String defaultFormatter = XPathUtils.evalString("/indexer/formatters/@default", doc);
        if (defaultFormatter.length() != 0) {
            conf.getFormatters().setDefaultFormatter(defaultFormatter);
        }
    }

    private Formatter instantiateFormatter(String className) throws IndexerConfException {
        ClassLoader contextCL = Thread.currentThread().getContextClassLoader();
        Class formatterClass;
        try {
            formatterClass = contextCL.loadClass(className);
        } catch (ClassNotFoundException e) {
            throw new IndexerConfException("Error loading formatter class " + className + " from context class loader.",
                    e);
        }

        if (!Formatter.class.isAssignableFrom(formatterClass)) {
            throw new IndexerConfException(
                    "Specified formatter class does not implement Formatter interface: " + className);
        }

        try {
            return (Formatter) formatterClass.newInstance();
        } catch (Exception e) {
            throw new IndexerConfException("Error instantiating formatter class " + className, e);
        }
    }

    private Map<String, String> parseVariantPropertiesPattern(Element caseEl, String attrName) throws Exception {
        String variant = DocumentHelper.getAttribute(caseEl, attrName, false);

        if (variant == null)
            return null;

        Map<String, String> varPropsPattern = new HashMap<String, String>();

        for (String prop : COMMA_SPLITTER.split(variant)) {
            int eqPos = prop.indexOf("=");
            if (eqPos != -1) {
                String propName = prop.substring(0, eqPos);
                String propValue = prop.substring(eqPos + 1);
                if (propName.equals("*")) {
                    throw new IndexerConfException(String.format("Error in " + attrName +
                            " attribute: the character '*' can only be used as wildcard, not as variant dimension " +
                            "name, attribute = %1$s, at: %2$s", variant, LocationAttributes.getLocation(caseEl)));
                }
                varPropsPattern.put(propName, propValue);
            } else {
                varPropsPattern.put(prop, null);
            }
        }

        return varPropsPattern;
    }

    private Set<SchemaId> parseVersionTags(String vtagsSpec) throws IndexerConfException, InterruptedException {
        Set<SchemaId> vtags = new HashSet<SchemaId>();

        if (vtagsSpec == null)
            return vtags;

        for (String tag : COMMA_SPLITTER.split(vtagsSpec)) {
            try {
                vtags.add(typeManager.getFieldTypeByName(VersionTag.qname(tag)).getId());
            } catch (FieldTypeNotFoundException e) {
                throw new IndexerConfException("unknown vtag used in indexer configuration: " + tag);
            } catch (RepositoryException e) {
                throw new IndexerConfException("error loading field type for vtag: " + tag, e);
            }
        }

        return Collections.unmodifiableSet(vtags);
    }

    private void buildIndexFields() throws Exception {
        conf.setIndexFields(buildIndexFields(INDEX_FIELDS.get().evalAsNativeElement(doc)));
    }

    public IndexFields buildIndexFields(Element el) throws Exception {
        IndexFields indexFields = new IndexFields();
        if (el != null) {
            addChildNodes(el, indexFields, "match", "field", "forEach");
        }
        return indexFields;
    }

    private MatchNode buildMatchNode(Element el) throws Exception {
        RecordMatcher recordMatcher = parseRecordMatcher(el);
        MatchNode matchNode = new MatchNode(recordMatcher);

        addChildNodes(el, matchNode, "match", "field", "forEach");
        return matchNode;
    }

    /**
     * @param forwardVariantDimensions in case this is an index field which is part of a foreach, these are the forward
     *                                 variant dimensions used in the foreach, and thus the ones that can be used to
     *                                 build a name from a template. Otherwise <code>null</code>.
     */
    private IndexField buildIndexField(Element el, Set<String> forwardVariantDimensions) throws Exception {
        String nameAttr = DocumentHelper.getAttribute(el, "name", true);
        String valueExpr = DocumentHelper.getAttribute(el, "value", true);

        final Set<QName> supportedFields = new HashSet<QName>();
        supportedFields.addAll(systemFields.getAll());
        supportedFields.addAll(getAllRepositoryFields());

        NameTemplate name = new NameTemplateParser(repositoryManager, systemFields)
                .parse(el, nameAttr, new FieldNameTemplateValidator(forwardVariantDimensions, supportedFields));

        return new IndexField(name, buildValue(el, valueExpr));
    }

    private Set<QName> getAllRepositoryFields() throws RepositoryException, InterruptedException {
        final Set<QName> result = new HashSet<QName>();
        for (FieldType fieldType : repositoryManager.getTypeManager().getFieldTypes()) {
            result.add(fieldType.getName());
        }
        return result;
    }

    private ForEachNode buildForEachNode(Element el) throws Exception {
        String expr = DocumentHelper.getAttribute(el, "expr", true);

        Follow follow = null;
        try {
            follow = parseFollow(el, expr);
        } catch (Exception e) {
            throw new IndexerConfException("Failed to process forEach element at " + LocationAttributes.getLocationString(el), e);
        }
        ForEachNode forEachNode = new ForEachNode(systemFields, follow);
        addChildNodes(el, forEachNode, "field", "match", "forEach");
        return forEachNode;
    }

    public void addChildNodes(Element el, ContainerMappingNode parent, String... allowedTagNames) throws Exception {
        Set<String> allowed = Sets.newHashSet(allowedTagNames);

        for (Element childEl: DocumentHelper.getElementChildren(el)) {
            String name = childEl.getTagName();

            if (!allowed.contains(name)) {
                throw new IndexerConfException(String.format("Unexpected tag name '%s' while parsing indexerconf", childEl.getTagName()));
            }

            if (name.equals("fields")) {
                parent.addChildNode(buildIndexFields(childEl));
            } else if (name.equals("match")) {
                parent.addChildNode(buildMatchNode(childEl));
            } else if (name.equals("field")) {
                final IndexField indexField;
                if (parent instanceof ForEachNode && ((ForEachNode) parent).getFollow() instanceof ForwardVariantFollow) {
                    indexField = buildIndexField(childEl, ((ForwardVariantFollow) ((ForEachNode)parent).getFollow()).getDimensions().keySet());
                } else {
                    indexField = buildIndexField(childEl, null);
                }
                parent.addChildNode(indexField);
            } else if (name.equals("forEach")) {
                parent.addChildNode(buildForEachNode(childEl));
            } else {
                throw new IndexerConfException(String.format("Unexpected tag name '%s' while parsing indexerconf", childEl.getTagName()));
            }
        }
    }

    private void buildDynamicFields() throws Exception {
        List<Element> fields = DYNAMIC_INDEX_FIELDS.get().evalAsNativeElementList(doc);
        for (Element fieldEl : fields) {
            String matchNamespaceAttr = DocumentHelper.getAttribute(fieldEl, "matchNamespace", false);
            String matchNameAttr = DocumentHelper.getAttribute(fieldEl, "matchName", false);
            String matchTypeAttr = DocumentHelper.getAttribute(fieldEl, "matchType", false);
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

            TypePattern matchTypes = null;
            if (matchTypeAttr != null) {
                matchTypes = new TypePattern(matchTypeAttr);
            }

            Set<Scope> matchScopes = null;
            if (matchScopeAttr != null) {
                matchScopes = EnumSet.noneOf(Scope.class);
                for (String scope : COMMA_SPLITTER.split(matchScopeAttr)) {
                    matchScopes.add(Scope.valueOf(scope));
                }
                if (matchScopes.isEmpty()) {
                    matchScopes = null;
                }
            }

            // Be gentle to users of Lily 1.0 and warn them about attributes that are not supported anymore
            if (DocumentHelper.getAttribute(fieldEl, "matchMultiValue", false) != null) {
                log.warn("The attribute matchMultiValue on dynamicField is not supported anymore, it will be ignored.");
            }
            if (DocumentHelper.getAttribute(fieldEl, "matchHierarchical", false) != null) {
                log.warn(
                        "The attribute matchHierarchical on dynamicField is not supported anymore, it will be ignored.");
            }

            Set<String> variables = new HashSet<String>();
            variables.add("namespace");
            variables.add("name");
            variables.add("type");
            variables.add("baseType");
            variables.add("nestedType");
            variables.add("nestedBaseType");
            variables.add("deepestNestedBaseType");
            if (matchName != null && matchName.hasWildcard())
                variables.add("nameMatch");
            if (matchNamespace != null && matchNamespace.hasWildcard())
                variables.add("namespaceMatch");

            NameTemplate name;
            try {
                name = new NameTemplateParser().parse(fieldEl, nameAttr, new DynamicFieldNameTemplateValidator(variables));
            } catch (NameTemplateException nte) {
                throw new IndexerConfException("Error in name template: " + nameAttr + " at " + LocationAttributes.getLocationString(fieldEl), nte);
            }

            boolean extractContent = DocumentHelper.getBooleanAttribute(fieldEl, "extractContent", false);

            String formatter = DocumentHelper.getAttribute(fieldEl, "formatter", false);
            if (formatter != null && !conf.getFormatters().hasFormatter(formatter)) {
                throw new IndexerConfException("Formatter does not exist: " + formatter + " at " +
                        LocationAttributes.getLocationString(fieldEl));
            }

            boolean continue_ = DocumentHelper.getBooleanAttribute(fieldEl, "continue", false);

            DynamicIndexField field = new DynamicIndexField(matchNamespace, matchName, matchTypes,
                    matchScopes, name, extractContent, continue_, formatter);

            conf.addDynamicIndexField(field);
        }
    }

    private void validateName(String name) throws IndexerConfException {
        //FIXME: seems like a useful validation, but not called any more?
        if (name.startsWith("lily.")) {
            throw new IndexerConfException("names starting with 'lily.' are reserved for internal uses. Name: " + name);
        }
    }

    private Value buildValue(Element fieldEl, String valueExpr) throws Exception {
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

        // A dereference expression is specified as "somelink=>somelink=>somefield"

        if (valueExpr.contains("=>")) {
            //
            // A dereference field
            //
            value = buildDerefValue(fieldEl, valueExpr, extractContent, formatter);
        } else {
            //
            // A plain field
            //
            value = new FieldValue(ConfUtil.getFieldType(valueExpr, fieldEl, systemFields, typeManager), extractContent, formatter);
        }

        if (extractContent &&
                !value.getTargetFieldType().getValueType().getDeepestValueType().getBaseName().equals("BLOB")) {
            throw new IndexerConfException("extractContent is used for a non-blob value at "
                    + LocationAttributes.getLocation(fieldEl));
        }

        return value;
    }

    private Value buildDerefValue(Element fieldEl, String valueExpr, boolean extractContent, String formatter)
            throws Exception {

        final String[] derefParts = parseDerefParts(fieldEl, valueExpr);
        final List<Follow> follows = parseFollows(fieldEl, valueExpr, derefParts);
        final Value value = buildValue(fieldEl, derefParts[derefParts.length - 1]);

        boolean lastFollowIsRecord = false;
        for (Follow follow: follows) {
            if (lastFollowIsRecord) {
                if (follow instanceof VariantFollow ||
                        follow instanceof ForwardVariantFollow ||
                        follow instanceof MasterFollow) {
                    String locationString = LocationAttributes.getLocationString(fieldEl);
                    throw new IndexerConfException("In deref expressions, a variant(+/-/master) follow" +
                    		" cannot follow after a record field. Location: " + locationString);
                }
            }
            if (follow instanceof RecordFieldFollow) {
                lastFollowIsRecord = true;
            } else {
                lastFollowIsRecord = false;
            }
        }

        // If the last follow is a RecordFieldFollow, we check that the Value isn't something which requires a real Record
        if (lastFollowIsRecord) {
            SchemaId fieldDependency = value.getFieldDependency();
            if (systemFields.isSystemField(fieldDependency)) {
                checkSystemFieldUsage(fieldEl, valueExpr, fieldDependency, new QName(SystemFields.NS, "id"));
                checkSystemFieldUsage(fieldEl, valueExpr, fieldDependency, new QName(SystemFields.NS, "link"));
            }
        }

        final FieldType fieldType = constructDerefFieldType(fieldEl, valueExpr, derefParts);
        final DerefValue deref = new DerefValue(follows, value, fieldType, extractContent, formatter);

        deref.init(typeManager);
        return deref;
    }

    private void checkSystemFieldUsage(Element fieldEl, String valueExpr, SchemaId fieldDependency, QName field)
            throws FieldTypeNotFoundException, IndexerConfException {
        if (fieldDependency.equals(systemFields.get(field))) {
            throw new IndexerConfException("In dereferencing, " + field + " cannot follow on record-type field." +
                    " Deref expression: '" + valueExpr + "' at " + LocationAttributes.getLocation(fieldEl));

        }
    }

    private List<Follow> parseFollows(Element fieldEl, String valueExpr, String[] derefParts) throws Exception {
        List<Follow> follows = new ArrayList<Follow>();

        try {
            for (int i = 0; i < derefParts.length - 1; i++) {
                String derefPart = derefParts[i];

                // A deref expression can navigate through 5 kinds of 'links':
                //  - a link stored in a link field (detected based on presence of a colon)
                //  - a nested record
                //  - a link to the master variant (if it's the literal string 'master')
                //  - a link to a less-dimensioned variant
                //  - a link to a more-dimensioned variant

                follows.add(parseFollow(fieldEl, derefPart));
            }
        } catch (Exception e) {
            throw new IndexerConfException("Failed to parse deref expression at " + LocationAttributes.getLocationString(fieldEl));
        }

        return follows;
    }

    private Follow parseFollow(Element fieldEl, String derefPart) throws IndexerConfException, InterruptedException, RepositoryException {
        if (derefPart.contains(":")) { // It's a field name
            return processFieldDeref(fieldEl, derefPart);
        } else if (derefPart.equals("master")) { // Link to master variant
            return new MasterFollow();
        } else if (derefPart.trim().startsWith("-")) {  // Link to less dimensioned variant
            return processLessDimensionedVariantsDeref(derefPart);
        } else if (derefPart.trim().startsWith("+")) {  // Link to more dimensioned variant
            return processMoreDimensionedVariantsDeref(derefPart);
        } else {
            throw new IndexerConfException("I don't know how handle the part '" + derefPart + "'");
        }
    }

    private String[] parseDerefParts(Element fieldEl, String valueExpr) throws IndexerConfException {
        //
        // Split, normalize, validate the input
        //
        String[] derefParts = Iterables.toArray(DEREF_SIGN_SPLITTER.split(valueExpr), String.class);
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
        return derefParts;
    }

    private FieldType constructDerefFieldType(Element fieldEl, String valueExpr, String[] derefParts)
            throws IndexerConfException, InterruptedException, RepositoryException {
        //
        // Last element in the list should be a field
        //
        QName targetFieldName;
        try {
            targetFieldName = ConfUtil.parseQName(derefParts[derefParts.length - 1], fieldEl);
        } catch (IndexerConfException e) {
            throw new IndexerConfException("Dereference expression does not end on a valid field name. " +
                    "Expression: '" + valueExpr + "' at " + LocationAttributes.getLocationString(fieldEl), e);
        }
        return ConfUtil.getFieldType(targetFieldName, systemFields, typeManager);
    }

    private Follow processFieldDeref(Element fieldEl, String derefPart)
            throws IndexerConfException, InterruptedException, RepositoryException {
        FieldType followField = ConfUtil.getFieldType(derefPart, fieldEl, systemFields, typeManager);

        String type = followField.getValueType().getBaseName();
        if (type.equals("LIST")) {
            type = followField.getValueType().getNestedValueType().getBaseName();
        }

        if (type.equals("RECORD")) {
            return new RecordFieldFollow(followField);
        } else if (type.equals("LINK")) {
            return new LinkFieldFollow(followField);
        } else {
            throw new IndexerConfException("Dereferencing is not possible on field of type " +
                    followField.getValueType().getName() + ". Field: '" + derefPart);
        }
    }

    private Follow processLessDimensionedVariantsDeref(String derefPart) throws IndexerConfException {
        // The variant dimensions are specified in a syntax like "-var1,-var2,-var3"
        boolean validConfig = true;
        Set<String> dimensions = new HashSet<String>();
        for (String op : COMMA_SPLITTER.split(derefPart)) {
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
            throw new IndexerConfException("Invalid specification of variants to follow: '" + derefPart);
        }

        return new VariantFollow(dimensions);
    }

    private Follow processMoreDimensionedVariantsDeref(String derefPart) throws IndexerConfException {
        // The variant dimension is specified in a syntax like "+var1=boo,+var2"
        boolean validConfig = true;
        Map<String, String> dimensions = new HashMap<String, String>();
        for (String op : COMMA_SPLITTER.split(derefPart)) {
            if (op.length() > 1 && op.startsWith("+")) {
                final Iterator<String> keyAndValue = EQUAL_SIGN_SPLITTER.split(op).iterator();
                if (keyAndValue.hasNext()) {
                    final String key = keyAndValue.next().substring(1); // ignore leading '+'
                    if (keyAndValue.hasNext()) {
                        // there is an equal sign -> key and value
                        final String value = keyAndValue.next();
                        dimensions.put(key, value);
                    } else {
                        // no equal sign -> only key without value
                        dimensions.put(key, null);
                    }
                } else {
                    // nothing at all?
                    validConfig = false;
                    break;
                }
            } else {
                validConfig = false;
                break;
            }
        }
        if (dimensions.size() == 0)
            validConfig = false;

        if (!validConfig) {
            throw new IndexerConfException("Invalid specification of variants to follow: '" + derefPart);
        }

        return new ForwardVariantFollow(dimensions);
    }

    private void validate(Document document) throws IndexerConfException {
        try {
            SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            URL url = getClass().getClassLoader()
                    .getResource("org/lilyproject/indexer/model/indexerconf/indexerconf.xsd");
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
            URL url = IndexerConfBuilder.class.getClassLoader()
                    .getResource("org/lilyproject/indexer/model/indexerconf/indexerconf.xsd");
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
        private final StringBuilder builder = new StringBuilder();

        @Override
        public void warning(SAXParseException exception) throws SAXException {
        }

        @Override
        public void error(SAXParseException exception) throws SAXException {
            addException(exception);
        }

        @Override
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
