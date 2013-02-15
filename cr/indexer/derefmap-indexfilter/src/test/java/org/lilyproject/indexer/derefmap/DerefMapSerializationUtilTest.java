package org.lilyproject.indexer.derefmap;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

/**
 *
 */
public class DerefMapSerializationUtilTest {

    private final IdGenerator ids = new IdGeneratorImpl();

    private final DerefMapSerializationUtil serializationUtil = new DerefMapSerializationUtil(ids);

    @Test
    public void serializeFields() throws Exception {
        final Set<SchemaId> fields = new HashSet<SchemaId>();
        fields.add(ids.getSchemaId(UUID.randomUUID()));
        fields.add(ids.getSchemaId(UUID.randomUUID()));

        final Set<SchemaId> deserialized =
                serializationUtil.deserializeFields(serializationUtil.serializeFields(fields));

        assertEquals(fields, deserialized);
    }

    @Test
    public void serializeEntriesForward() throws Exception {
        final Set<DependencyEntry> dependencies = new HashSet<DependencyEntry>();
        dependencies.add(new DependencyEntry(ids.newRecordId("id1"), new HashSet<String>()));
        dependencies
                .add(new DependencyEntry(ids.newRecordId("id2", ImmutableMap.of("bar", "x")), Sets.newHashSet(
                        "foo")));

        final Set<DependencyEntry> deserialized = serializationUtil.deserializeDependenciesForward(
                serializationUtil.serializeDependenciesForward(dependencies));

        assertEquals(dependencies, deserialized);
    }

    @Test
    public void serializeVariantPropertiesPattern() throws Exception {
        final HashMap<String, String> pattern = new HashMap<String, String>();
        pattern.put("foo", null);
        pattern.put("bar", "x");

        final DerefMapVariantPropertiesPattern variantPropertiesPattern =
                new DerefMapVariantPropertiesPattern(pattern);

        final DerefMapVariantPropertiesPattern deserialized =
                serializationUtil.deserializeVariantPropertiesPattern(
                        serializationUtil.serializeVariantPropertiesPattern(variantPropertiesPattern));

        Assert.assertEquals(variantPropertiesPattern, deserialized);
    }

}
