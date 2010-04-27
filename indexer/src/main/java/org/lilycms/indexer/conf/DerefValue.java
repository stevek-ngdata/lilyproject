package org.lilycms.indexer.conf;

import org.lilycms.repository.api.*;
import org.lilycms.repoutil.VersionTag;

import java.util.*;

public class DerefValue implements Value {
    private List<Follow> follows = new ArrayList<Follow>();
    private QName fieldName;

    protected DerefValue(QName fieldName) {
        this.fieldName = fieldName;
    }

    protected void addFieldFollow(QName fieldName) {
        follows.add(new FieldFollow(fieldName));
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
    public QName getTargetField() {
        return fieldName;
    }

    public static interface Follow {
        List<Record> eval(Record record, Repository repository, String vtag);
    }

    public static class FieldFollow implements Follow {
        QName fieldName;

        public FieldFollow(QName fieldName) {
            this.fieldName = fieldName;
        }

        public List<Record> eval(Record record, Repository repository, String vtag) {
            if (!record.hasField(fieldName))
                return null;

            Object value = record.getField(fieldName);
            if (value instanceof RecordId) {
                Record linkedRecord = resolveRecordId((RecordId)value, vtag, repository);
                return linkedRecord == null ? null : Collections.singletonList(linkedRecord);
            } else if (value instanceof List && ((List)value).size() > 0 && ((List)value).get(0) instanceof RecordId) {
                List list = (List)value;
                List<Record> result = new ArrayList<Record>(list.size());
                for (Object recordId : list) {
                    Record linkedRecord = resolveRecordId((RecordId)recordId, vtag, repository);
                    if (linkedRecord != null) {
                        result.add(linkedRecord);
                    }
                }
                return list.isEmpty() ? null : result;
            }
            return null;
        }

        public QName getFieldName() {
            return fieldName;
        }

        private Record resolveRecordId(RecordId recordId, String vtag, Repository repository) {
            try {
                // TODO we could limit this to only load the field necessary for the next follow
                return VersionTag.getRecord(recordId, vtag, repository);
            } catch (Exception e) {
                return null;
            }
        }
    }

    public static class MasterFollow implements Follow {
        public List<Record> eval(Record record, Repository repository, String vtag) {
            if (record.getId().isMaster())
                return null;

            RecordId masterId = record.getId().getMaster();

            try {
                Record master = VersionTag.getRecord(masterId, vtag, repository);
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

        public List<Record> eval(Record record, Repository repository, String vtag) {
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
                Record lessDimensionedRecord = VersionTag.getRecord(resolvedRecordId, vtag, repository);
                return lessDimensionedRecord == null ? null : Collections.singletonList(lessDimensionedRecord);
            } catch (Exception e) {
                return null;
            }
        }

        public Set<String> getDimensions() {
            return dimensions;
        }
    }

    public List<String> eval(Record record, Repository repository, String vtag) {
        List<Record> records = new ArrayList<Record>();
        records.add(record);

        for (Follow follow : follows) {
            List<Record> linkedRecords = new ArrayList<Record>();

            for (Record item : records) {
                List<Record> evalResult = follow.eval(item, repository, vtag);
                if (evalResult != null) {
                    linkedRecords.addAll(evalResult);
                }
            }

            records = linkedRecords;
        }

        if (records.isEmpty())
            return null;

        List<String> result = new ArrayList<String>();
        for (Record item : records) {
            if (item.hasField(fieldName)) {
                Object value = item.getField(fieldName);
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

    public QName getFieldDependency() {
        if (follows.get(0) instanceof FieldFollow) {
            return ((FieldFollow)follows.get(0)).fieldName;
        } else {
            // A follow-variant is like a link to another document, but the link can never change as the
            // identity of the document never changes. Therefore, there is no dependency on a field.
            return null;
        }
    }

}
