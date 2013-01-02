package org.lilyproject.indexer.model.indexerconf.formatter;

import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.lilyproject.indexer.model.indexerconf.IndexValue;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.impl.valuetype.ListValueType;
import org.lilyproject.repository.impl.valuetype.StringValueType;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HierarchicalFacetPrefixFormatterTest {
    @Test
    public void testSingValue() throws Exception {
        ValueType valueType = new StringValueType();
        FieldType fieldType = mock(FieldType.class);
        when(fieldType.getValueType()).thenReturn(valueType);

        HierarchicalFacetPrefixFormatter formatter = new HierarchicalFacetPrefixFormatter();
        IndexValue value = new IndexValue(null, fieldType, "foo/bar");
        List<String> result = formatter.format(Lists.newArrayList(value), null);

        assertEquals(2, result.size());
        assertEquals("0/foo", result.get(0));
        assertEquals("1/foo/bar", result.get(1));
    }

    @Test
    public void testMultiValue() throws Exception {
        TypeManager typeManager = mock(TypeManager.class);
        when(typeManager.getValueType("STRING")).thenReturn(new StringValueType());

        ValueType valueType = new ListValueType(typeManager, "STRING");
        FieldType fieldType = mock(FieldType.class);
        when(fieldType.getValueType()).thenReturn(valueType);

        HierarchicalFacetPrefixFormatter formatter = new HierarchicalFacetPrefixFormatter();
        List<String> result = formatter.format(Lists.newArrayList(
                new IndexValue(null, fieldType, "foo/bar"),
                new IndexValue(null, fieldType, "bar/foo")), null);

        assertEquals(4, result.size());
        assertEquals("0/foo", result.get(0));
        assertEquals("1/foo/bar", result.get(1));
        assertEquals("0/bar", result.get(2));
        assertEquals("1/bar/foo", result.get(3));
    }
}
