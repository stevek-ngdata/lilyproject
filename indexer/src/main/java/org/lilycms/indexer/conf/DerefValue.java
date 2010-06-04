package org.lilycms.indexer.conf;

import org.lilycms.repository.api.*;
import org.lilycms.repoutil.VersionTag;

import java.util.*;

public class DerefValue implements Value {
    private List<Follow> follows = new ArrayList<Follow>();
    private FieldType fieldType;

    protected DerefValue(FieldType fieldType) {
        this.fieldType = fieldType;
    }

    protected void addFieldFollow(FieldType fieldType) {
        follows.add(new FieldFollow(fieldType));
    }

    protected void addMasterFollow() {
        follows.add(new MasterFollow());
    }

    protected void addVariantFollow(Set<String> dimensions) {
        follows.add(new VariantFollow(dimensions));
    }

    public List<Follow> getFollows() {
        return Collections.unmodifiableList(follows);
    }

    /**
     * Returns the field taken from the document to which the follow-expressions point, thus the last
     * field in the chain.
     */
    public FieldType getTargetField() {
        return fieldType;
    }

    public static interface Follow {
        List<IdRecord> eval(IdRecord record, Repository repository, String vtag);
    }

    public static class FieldFollow implements Follow {
        FieldType fieldType;

        public FieldFollow(FieldType fieldType) {
            this.fieldType = fieldType;
        }

        public List<IdRecord> eval(IdRecord record, Repository repository, String vtag) {
            if (!record.hasField(fieldType.getId())) {
                return null;
            }

            if (vtag.equals(VersionTag.VERSIONLESS_TAG) && fieldType.getScope() != Scope.NON_VERSIONED) {
                // From a versionless record, it is impossible to deref a versioned field.
                // This explicit check could be removed if in case of the versionless vtag we only read
                // the non-versioned fields of the record. However, it is not possible to do this right
                // now with the repository API.
                return null;
            }

            Object value = record.getField(fieldType.getId());
            if (value instanceof RecordId) {
                IdRecord linkedRecord = resolveRecordId((RecordId)value, vtag, repository);
                return linkedRecord == null ? null : Collections.singletonList(linkedRecord);
            } else if (value instanceof List && ((List)value).size() > 0 && ((List)value).get(0) instanceof RecordId) {
                List list = (List)value;
                List<IdRecord> result = new ArrayList<IdRecord>(list.size());
                for (Object recordId : list) {
                    IdRecord linkedRecord = resolveRecordId((RecordId)recordId, vtag, repository);
                    if (linkedRecord != null) {
                        result.add(linkedRecord);
                    }
                }
                return list.isEmpty() ? null : result;
            }
            return null;
        }

        public String getFieldId() {
            return fieldType.getId();
        }

        public FieldType getFieldType() {
            return fieldType;
        }

        private IdRecord resolveRecordId(RecordId recordId, String vtag, Repository repository) {
            try {
                // TODO we could limit this to only load the field necessary for the next follow
                return VersionTag.getIdRecord(recordId, vtag, repository);
            } catch (Exception e) {
                return null;
            }
        }
    }

    public static class MasterFollow implements Follow {
        public List<IdRecord> eval(IdRecord record, Repository repository, String vtag) {
            if (record.getId().isMaster())
                return null;

            RecordId masterId = record.getId().getMaster();

            try {
                IdRecord master = VersionTag.getIdRecord(masterId, vtag, repository);
                return master == null ? null : Collections.singletonList(master);
            } catch (Exception e) {
                return null;
            }
        }
    }

    public static class VariantFollow implements Follow {
        private Set<String> dimensions;

        public VariantFollow(Set<String> dimensions) {
            this.dimensions = dimensions;
        }

        public List<IdRecord> eval(IdRecord record, Repository repository, String vtag) {
            RecordId recordId = record.getId();

            Map<String, String> varProps = new HashMap<String, String>(recordId.getVariantProperties());

            for (String dimension : dimensions) {
                if (!varProps.containsKey(dimension)) {
                    return null;
                }
                varProps.remove(dimension);
            }

            RecordId resolvedRecordId = repository.getIdGenerator().newRecordId(recordId.getMaster(), varProps);

            try {
                IdRecord lessDimensionedRecord = VersionTag.getIdRecord(resolvedRecordId, vtag, repository);
                return lessDimensionedRecord == null ? null : Collections.singletonList(lessDimensionedRecord);
            } catch (Exception e) {
                return null;
            }
        }

        public Set<String> getDimensions() {
            return dimensions;
        }
    }

    public List<String> eval(IdRecord record, Repository repository, String vtag) {
        if (vtag.equals(VersionTag.VERSIONLESS_TAG) && fieldType.getScope() != Scope.NON_VERSIONED) {
            // From a versionless record, it is impossible to deref a versioned field.
            return null;
        }

        List<IdRecord> records = new ArrayList<IdRecord>();
        records.add(record);

        for (Follow follow : follows) {
            List<IdRecord> linkedRecords = new ArrayList<IdRecord>();

            for (IdRecord item : records) {
                List<IdRecord> evalResult = follow.eval(item, repository, vtag);
                if (evalResult != null) {
                    linkedRecords.addAll(evalResult);
                }
            }

            records = linkedRecords;
        }

        if (records.isEmpty())
            return null;

        List<String> result = new ArrayList<String>();
        for (IdRecord item : records) {
            if (item.hasField(fieldType.getId())) {
                Object value = item.getField(fieldType.getId());
                if (value != null) {
                    // TODO formatting of value
                    result.add(value.toString());
                }
            }
        }

        if (result.isEmpty())
            return null;

        return result;
    }

    public String getFieldDependency() {
        if (follows.get(0) instanceof FieldFollow) {
            return ((FieldFollow)follows.get(0)).fieldType.getId();
        } else {
            // A follow-variant is like a link to another document, but the link can never change as the
            // identity of the document never changes. Therefore, there is no dependency on a field.
            return null;
        }
    }

}
