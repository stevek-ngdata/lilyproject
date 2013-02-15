package org.lilyproject.runtime;

public enum SharingConflictResolution {
    
    HIGHEST("highest"),
    ERROR("error"),
    DONTSHARE("dontshare");
    
    private String name;
    
    private SharingConflictResolution(String name) {
        this.name = name;
    }
    
    public String getName() {
        return name;
    }
    
    public static SharingConflictResolution fromString(String name) {
        if (HIGHEST.name.equals(name))
            return HIGHEST;
        if (ERROR.name.equals(name))
            return ERROR;
        if (DONTSHARE.name.equals(name))
            return DONTSHARE;
        
        return null;
    }

}
