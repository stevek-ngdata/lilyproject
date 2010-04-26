package org.lilycms.repoutil;

import org.apache.commons.logging.LogFactory;
import org.lilycms.repository.api.*;
import org.lilycms.repository.api.exception.*;

import java.util.*;

/**
 * Version tag related utilities.
 */
public class VersionTag {

    /**
     * Namespace for field types that serve as version tags.
     */
    public static final String NS_VTAG = "org.lilycms.vtag";

    /**
     * A dummy tag used for documents which have no versions, and thus no tagged versions.
     */
    public static final String VERSIONLESS_TAG = "@@versionless";

    /**
     * Returns the vtags of a record, the key in the map is the field type ID of the vtag field, not its name.
     *
     * <p>Note that version numbers do not necessarily correspond to existing versions.
     */
    public static Map<String, Long> getTagsById(Record record, TypeManager typeManager) {
        Map<String, Long> vtags = new HashMap<String, Long>();

        for (Map.Entry<QName, Object> field : record.getFields().entrySet()) {
            // TODO: once getFieldTypeByName throws a FieldTypeNotFoundException, skip such fields
            FieldType fieldType = typeManager.getFieldTypeByName(field.getKey());

            if (isVersionTag(fieldType)) {
                vtags.put(fieldType.getId(), (Long)field.getValue());
            }
        }

        return vtags;
    }

    /**
     * Returns the vtags of a record, the key in the map is the name of the vtag field (without namespace).
     *
     * <p>Note that version numbers do not necessarily correspond to existing versions.
     */
    public static Map<String, Long> getTagsByName(Record record, TypeManager typeManager) {
        Map<String, Long> vtags = new HashMap<String, Long>();

        for (Map.Entry<QName, Object> field : record.getFields().entrySet()) {
            // TODO: once getFieldTypeByName throws a FieldTypeNotFoundException, skip such fields
            FieldType fieldType = typeManager.getFieldTypeByName(field.getKey());

            if (isVersionTag(fieldType)) {
                vtags.put(fieldType.getName().getName(), (Long)field.getValue());
            }
        }

        return vtags;
    }

    /**
     * Returns true if the given FieldType is a version tag.
     */
    public static boolean isVersionTag(FieldType fieldType) {
        return (fieldType.getName().getNamespace().equals(NS_VTAG)
                && fieldType.getScope() == Scope.NON_VERSIONED
                && fieldType.getValueType().isPrimitive()
                && fieldType.getValueType().getPrimitive().getName().equals("LONG"));
    }

    /**
     * Inverts a map containing version by tag to a map containing tags by version.
     */
    public static Map<Long, Set<String>> tagsByVersion(Map<String, Long> vtags) {
        Map<Long, Set<String>> result = new HashMap<Long, Set<String>>();

        for (Map.Entry<String, Long> entry : vtags.entrySet()) {
            Set<String> tags = result.get(entry.getValue());
            if (tags == null) {
                tags = new HashSet<String>();
                result.put(entry.getValue(), tags);
            }
            tags.add(entry.getKey());
        }

        return result;
    }

    /**
     * Filters the given set of fields to only those that are vtag fields.
     */
    public static Set<String> filterVTagFields(Set<String> fieldIds, TypeManager typeManager) throws RepositoryException {
        Set<String> result = new HashSet<String>();
        for (String field : fieldIds) {
            try {
                if (VersionTag.isVersionTag(typeManager.getFieldTypeById(field))) {
                    result.add(field);
                }
            } catch (FieldTypeNotFoundException e) {
                // ignore, if it does not exist, it can't be a version tag
            } catch (Throwable t) {
                LogFactory.getLog(VersionTag.class).error("Error loading field type to find out if it is a vtag field.", t);
            }
        }
        return result;
    }

    /**
     * Resolves a vtag to a version number for some record.
     *
     * <p>It does not assume the vtag exists, is really a vtag field, etc.
     *
     * <p>It should not be called for the @@versionless tag, since that cannot be resolved to a version number.
     *
     * <p>If the specified record would not exist, you will get an {@link RecordTypeNotFoundException}.
     *
     * @return null if the vtag does not exist, if it is not a valid vtag field, if the record does not exist,
     *         or if the record fails to load.
     */
    public static Long getVersion(RecordId recordId, String vtag, Repository repository) {
        QName vtagField = new QName(VersionTag.NS_VTAG, vtag);

        Record vtagRecord;
        try {
            vtagRecord = repository.read(recordId, Collections.singletonList(vtagField));
        } catch (Exception e) {
            return null;
        }

        // TODO this check should be done based on the ID of the field loaded in the record, once that is available (see #7)
        FieldType fieldType = repository.getTypeManager().getFieldTypeByName(vtagField);
        if (!VersionTag.isVersionTag(fieldType)) {
            return null;
        }

        if (!vtagRecord.hasField(vtagField))
            return null;

        return (Long)vtagRecord.getField(vtagField);
    }

}
