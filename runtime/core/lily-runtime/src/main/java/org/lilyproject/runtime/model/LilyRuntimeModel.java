package org.lilyproject.runtime.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This object describes the Lily Runtime system to be created by the LilyRuntime.
 *
 * <p>The LilyRuntimeModel should not be modified while a LilyRuntime that is using
 * it is running (TODO: maybe the LilyRuntime should create a clone of the config internally?).
 *
 */
public class LilyRuntimeModel {
    private List<ModuleDefinition> modules = new ArrayList<ModuleDefinition>();

    public LilyRuntimeModel() {
    }

    public List<ModuleDefinition> getModules() {
        return modules;
    }

    public void addModule(ModuleDefinition moduleDefinition) {
        modules.add(moduleDefinition);
    }

    public ModuleDefinition getModuleById(String id) {
        for (ModuleDefinition module : modules) {
            if (module.getId().equals(id))
                return module;
        }
        return null;
    }

    public void validate(List<ConfigError> configErrors) {
        // Check there are no modules with duplicate IDs
        Set<String> idSet = new HashSet<String>();
        for (ModuleDefinition entry : modules) {
            if (idSet.contains(entry.getId()))
                configErrors.add(new ConfigError("Duplicate module ID: " + entry.getId(), entry.getLocation()));
            entry.validate(configErrors, this);
            idSet.add(entry.getId());
        }
    }
   
    public String moduleInfo(String id){
        if (id == null || id.length() == 0 ){
            String result = "";
            int i = 1;
            for (ModuleDefinition module : modules) {
                result += String.format(" [%3d.] %s\n",  i++ , module.moduleInfo());
            }
            return result;
        }
        
        final ModuleDefinition module = getModuleById(id);
        if (module == null)
             return "No module with id: " + id;
        return module.moduleInfo();
    }

}
