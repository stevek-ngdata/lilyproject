package org.lilycms.repoutil;

import org.apache.commons.logging.LogFactory;
import org.lilycms.repository.api.*;
import org.lilycms.repository.api.exception.FieldTypeNotFoundException;
import org.lilycms.repository.api.exception.RepositoryException;

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

}
