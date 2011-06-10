package org.lilyproject.repository.api;

public enum ResponseStatus {
    /** The record was created. */
    CREATED,
    /** The record was updated. */
    UPDATED,
    /** The record did already contain the supplied data, it was not modified. */
    UP_TO_DATE,
    /** The record has not been updated because the supplied conditions were not satisfied. */
    CONFLICT
}
