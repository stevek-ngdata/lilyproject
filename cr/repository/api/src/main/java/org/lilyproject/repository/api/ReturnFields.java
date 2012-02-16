package org.lilyproject.repository.api;

import org.lilyproject.util.ArgumentValidator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Specifies what fields to return in a Record object.
 *
 * <p>This can be a manual enumeration of fields, or it can be ALL or NONE. Note that
 * ALL is usually the same as not specifying a ReturnFields object.
 * 
 * <p>For ALL and NONE, you can avoid instantiation by using {@link #ALL} and {@link #NONE}</p>
 *
 * <p>Instances of this class are immutable.</p>
 */
public class ReturnFields {
    private List<QName> fields;
    private Type type = Type.ALL;
    
    public static ReturnFields ALL = new ReturnFields(Type.ALL);
    public static ReturnFields NONE = new ReturnFields(Type.NONE);
    
    public enum Type {
        ALL, NONE, ENUM
    }
    
    public ReturnFields() {
        
    }
    
    public ReturnFields(Type type) {
        this.type = type;
    }
    
    public ReturnFields(List<QName> fields) {
        ArgumentValidator.notNull(fields, "fields");        
        this.fields = Collections.unmodifiableList(fields);
        this.type = Type.ENUM;
    }
    
    public ReturnFields(QName... fields) {
        this.fields = Collections.unmodifiableList(Arrays.asList(fields));
        this.type = Type.ENUM;
    }

    public Type getType() {
        return type;
    }

    /**
     * This method will only return non-null if {@link #getType()} returns {@link Type#ENUM}.
     */
    public List<QName> getFields() {
        return fields;
    }
}
