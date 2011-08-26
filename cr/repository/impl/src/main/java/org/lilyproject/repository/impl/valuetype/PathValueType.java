/*
 * Copyright 2011 Outerthought bvba
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
package org.lilyproject.repository.impl.valuetype;

import java.util.*;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.ArgumentValidator;

public class PathValueType extends AbstractValueType implements ValueType {
    
    public final static String NAME = "PATH";
    
    private ValueType valueType;

    private final String typeParams;
    private final String fullName;
    
    public PathValueType(TypeManager typeManager, String typeParams) throws RepositoryException, InterruptedException {
        ArgumentValidator.notNull(typeParams, "typeParams");
        this.typeParams = typeParams;
        this.fullName = NAME+"<"+typeParams+">";
        int indexOpenBracket = typeParams.indexOf('<');
        if (indexOpenBracket == -1) {
            this.valueType = typeManager.getValueType(typeParams);
        } else {
            String firstType = typeParams.substring(0, indexOpenBracket);
            String nestedTypeParams = typeParams.substring(indexOpenBracket + 1, typeParams.length()-1);
            this.valueType = typeManager.getValueType(firstType, nestedTypeParams);
        }
    }
    
    public PathValueType(TypeManager typeManager, DataInput typeParamsDataInput) throws RepositoryException, InterruptedException {
        this(typeManager, typeParamsDataInput.readUTF());
    }
    
    public String getName() {
        return NAME;
    }
    
    public String getFullName() {
        return fullName;
    }
    
    public ValueType getBaseValueType() {
        return valueType.getBaseValueType();
    }
    
    public ValueType getNestedValueType() {
        return valueType;
    }
    
    public int getNestingLevel() {
        return 1 + valueType.getNestingLevel();
    }

    @SuppressWarnings("unchecked")
    public HierarchyPath read(DataInput dataInput) throws UnknownValueTypeEncodingException, RepositoryException, InterruptedException {
        int nrOfValues = dataInput.readInt();
        List<Object> result = new ArrayList<Object>(nrOfValues);
        for (int i = 0 ; i < nrOfValues; i++) {
            result.add(valueType.read(dataInput));
        }
        return new HierarchyPath(result.toArray(new Object[result.size()]));
    }

    public void write(Object value, DataOutput dataOutput) throws RepositoryException, InterruptedException {
        Object[] elements = ((HierarchyPath) value).getElements();
        dataOutput.writeInt(elements.length);
        for (Object element : elements) {
            valueType.write(element, dataOutput);
        }
    }

    public Class getType() {
        return HierarchyPath.class;
    }

    @Override
    public Comparator getComparator() {
        return null;
    }

    @Override
    public void encodeTypeParams(DataOutput dataOutput) {
        dataOutput.writeUTF(typeParams);
    }
    
    @Override
    public byte[] getTypeParams() {
        DataOutput dataOutput = new DataOutputImpl();
        encodeTypeParams(dataOutput);
        return dataOutput.toByteArray();
    }
    
    @Override
    public Set<Object> getValues(Object value) {
        Set<Object> result = new HashSet<Object>();
        for (Object element : ((HierarchyPath) value).getElements()) {
            result.addAll(valueType.getValues(element));
        } 
        return result;
    }
    
    @Override
    public boolean isHierarchical() {
        return true;
    }

    //
    // Factory
    //
    public static ValueTypeFactory factory(TypeManager typeManager) {
        return new PathValueTypeFactory(typeManager);
    }
    
    public static class PathValueTypeFactory implements ValueTypeFactory {
        
        private TypeManager typeManager;

        public PathValueTypeFactory(TypeManager typeManager) {
            this.typeManager = typeManager;
        }
        
        @Override
        public ValueType getValueType(String typeParams) throws RepositoryException, InterruptedException {
            return new PathValueType(typeManager, typeParams);
        }
        
        @Override
        public ValueType getValueType(DataInput dataInput) throws RepositoryException, InterruptedException {
            return new PathValueType(typeManager, dataInput);
        }
    }
}
