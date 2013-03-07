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
package org.lilyproject.repository.impl.filter;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.lilyproject.repository.api.CompareOp;

public class HBaseRecordFilterUtil {
    private HBaseRecordFilterUtil() {
    }

    public static CompareFilter.CompareOp translateCompareOp(CompareOp compareOp) {
        CompareFilter.CompareOp result;
        switch (compareOp) {
            case EQUAL:
                result = CompareFilter.CompareOp.EQUAL;
                break;
            case NOT_EQUAL:
                result = CompareFilter.CompareOp.NOT_EQUAL;
                break;
            case GREATER:
                result = CompareFilter.CompareOp.GREATER;
                break;
            case GREATER_OR_EQUAL:
                result = CompareFilter.CompareOp.GREATER_OR_EQUAL;
                break;
            case LESS:
                result = CompareFilter.CompareOp.LESS;
                break;
            case LESS_OR_EQUAL:
                result = CompareFilter.CompareOp.LESS_OR_EQUAL;
                break;
            default:
                throw new RuntimeException("Unrecognized CompareOp: " + compareOp);
        }
        return result;
    }
}
