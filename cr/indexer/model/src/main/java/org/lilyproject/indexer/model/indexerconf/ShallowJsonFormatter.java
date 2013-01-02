/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.indexer.model.indexerconf;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import org.codehaus.jackson.JsonGenerator;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.util.json.JsonFormat;


/**
 * A formatter which renders a record as json.
 * <p/>
 * <p>This is useful when you want to retrieve records directly from SOLR</p>.
 * <p/>
 * <p>Limitation: Only shallow serialization is done: If a record has LIST or PATH members,
 * the values will be serialized according to the nested valuetype, joined by " " or "/" respectively.</p>
 */
public class ShallowJsonFormatter extends DefaultFormatter {

    @Override
    protected ValueFormatter getFormatter(ValueType valueType) {
        if ("RECORD".equals(valueType.getBaseName())) {
            return RECORD_KEY_VALUE_FORMATTER;
        } else {
            return super.getFormatter(valueType);
        }

    }

    protected static class RecordKeyValueFormatter implements ValueFormatter {
        @Override
        public String format(Object record, ValueType valueType, FormatContext formatCtx) throws InterruptedException {
            TypeManager typeManager = formatCtx.repository.getTypeManager();

            StringWriter writer = new StringWriter();
            try {
                JsonGenerator gen = JsonFormat.JSON_FACTORY.createJsonGenerator(writer);

                gen.writeStartObject();
                for (Map.Entry<QName, Object> field : ((Record)record).getFields().entrySet()) {
                    ValueType fieldValueType;
                    try {
                        fieldValueType = typeManager.getFieldTypeByName(field.getKey()).getValueType();
                    } catch (RepositoryException e) {
                        continue;
                    }

                    String result = formatCtx.format(field.getValue(), fieldValueType, formatCtx);
                    if (result != null) {
                        gen.writeStringField(field.getKey().getName(), result);
                    }
                }
                gen.writeEndObject();
                gen.flush();
            } catch (IOException e) {
                throw new InterruptedException(e.getMessage());
            }

            return returnBuilderResult(new StringBuilder(writer.toString()));
        }
    }

    private static ValueFormatter RECORD_KEY_VALUE_FORMATTER = new RecordKeyValueFormatter();
}
