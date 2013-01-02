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

import java.util.List;

/**
 * A formatter which only keeps the first entry of multi-valued values.
 * <p/>
 * <p>One case where this is useful is when you want to be able to sort
 * on LIST-type fields which only sometimes contain multiple values,
 * in this case you could decide to sort on the first value.
 */
public class FirstValueFormatter extends DefaultFormatter {

    @Override
    protected List<IndexValue> filterValues(List<IndexValue> indexValues) {
        return indexValues.isEmpty() ? indexValues : indexValues.subList(0, 1);
    }

}
