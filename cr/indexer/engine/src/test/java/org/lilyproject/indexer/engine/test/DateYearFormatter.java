/*
 * Copyright 2010 Outerthought bvba
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
package org.lilyproject.indexer.engine.test;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.lilyproject.indexer.model.indexerconf.DefaultFormatter;
import org.lilyproject.repository.api.ValueType;

public class DateYearFormatter extends DefaultFormatter {
    private static ValueFormatter DATE_FORMATTER = new DateFormatter();
    private static ValueFormatter DATETIME_FORMATTER = new DateTimeFormatter();

    @Override
    protected ValueFormatter getFormatter(ValueType valueType) {
        if (valueType.getBaseName().equals("DATE")) {
            return DATE_FORMATTER;
        } else if (valueType.getBaseName().equals("DATETIME")) {
            return DATETIME_FORMATTER;
        } else {
            return super.getFormatter(valueType);
        }
    }

    protected static class DateFormatter implements ValueFormatter {
        @Override
        public String format(Object value, ValueType valueType, FormatContext formatCtx) throws InterruptedException {
            LocalDate date = (LocalDate)value;
            return String.valueOf(date.getYear());
        }
    }

    protected static class DateTimeFormatter implements ValueFormatter {
        @Override
        public String format(Object value, ValueType valueType, FormatContext formatCtx) throws InterruptedException {
            DateTime dateTime = (DateTime)value;
            return String.valueOf(dateTime.getYear());
        }
    }
}
