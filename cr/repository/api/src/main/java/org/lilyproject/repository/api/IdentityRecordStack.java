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
package org.lilyproject.repository.api;

import java.util.ArrayList;
import java.util.List;

/**
 * A stack of records, supporting a contains operation that checks if a record
 * is contained in this stack based on its Java object identity (equals operator
 * rather than equals method).
 */
public class IdentityRecordStack {
    private Record first;
    private List<Record> stack;

    public IdentityRecordStack() {
    }

    /**
     * Convenience constructor to make a stack and directly push a first entry in it.
     */
    public IdentityRecordStack(Record firstEntry) {
        this.first = firstEntry;
    }
    
    public void push(Record record) {
        if (first == null) {
            first = record;
        } else {
            if (stack == null) {
                stack = new ArrayList<Record>(4);
            }
            stack.add(record);
        }
    }
    
    public Record pop() {
        if (stack != null && !stack.isEmpty()) {
            return stack.remove(stack.size() - 1);
        } else if (first != null) {
            Record result = first;
            first = null;
            return result;
        } else {
            throw new IllegalStateException("stack is empty");
        }
    }

    public boolean contains(Record record) {
        if (first == record) {
            return true;
        }
        
        if (stack != null) {
            for (Record entry : stack) {
                if (entry == record) {
                    return true;
                }
            }
        }
        
        return false;
    }
}
