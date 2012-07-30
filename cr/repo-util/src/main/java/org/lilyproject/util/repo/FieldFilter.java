package org.lilyproject.util.repo;

import org.lilyproject.repository.api.FieldType;

public interface FieldFilter {
    static final FieldFilter PASS_ALL_FIELD_FILTER = new FieldFilter() {
        @Override
        public boolean accept(FieldType fieldtype) {
            return true;
        }
    };

    boolean accept(FieldType fieldtype);
}
