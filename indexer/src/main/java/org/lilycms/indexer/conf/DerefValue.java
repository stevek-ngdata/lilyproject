package org.lilycms.indexer.conf;

import org.lilycms.repository.api.*;
import org.lilycms.repoutil.VersionTag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DerefValue implements Value {
    private List<Follow> follows = new ArrayList<Follow>();
    private QName fieldName;

    protected DerefValue(QName fieldName) {
        this.fieldName = fieldName;
    }

    protected void addFieldFollow(QName fieldName) {
        follows.add(new FieldFollow(fieldName));
    }

    private static interface Follow {
        List<Record> eval(Record record, Repository repository, String vtag);
    }

    private static class FieldFollow implements Follow {
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
                return linkedRecord == null ? null : Collections.<Record>singletonList(linkedRecord);
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

        private Record resolveRecordId(RecordId recordId, String vtag, Repository repository) {
            if (vtag.equals(VersionTag.VERSIONLESS_TAG)) {
                try {
                    return repository.read(recordId);
                } catch (Exception e) {
                    return null;
                }
            } else {
                Long version = VersionTag.getVersion(recordId, vtag, repository);
                if (version == null) {
                    return null;
                }

                try {
                    return repository.read(recordId, version, Collections.singletonList(fieldName));
                } catch (Exception e) {
                    return null;
                }
            }
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
        return ((FieldFollow)follows.get(0)).fieldName;
    }

}
