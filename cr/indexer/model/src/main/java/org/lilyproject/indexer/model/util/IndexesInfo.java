package org.lilyproject.indexer.model.util;

import org.lilyproject.repository.api.QName;

import java.util.Collection;
import java.util.Set;

/**
 * Utility bean providing access to the parsed indexerconfs for all defined indexes. It keeps
 * these permanently available for fast access, and updates them when the indexer model changes.
 */
public interface IndexesInfo {
    Collection<IndexInfo> getIndexInfos();

    Set<QName> getRecordFilterFieldDependencies();

    boolean getRecordFilterDependsOnRecordType();
}
