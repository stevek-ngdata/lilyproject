package org.lilyproject.tools.tester;

import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.lilyproject.clientmetrics.Metrics;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.Repository;

public class TestActionFactory {

    Map<String, Class> actionClasses = new HashMap<String, Class>();
    
    public TestActionFactory() {
        actionClasses.put("create", CreateAction.class);
        actionClasses.put("read", ReadAction.class);
        actionClasses.put("update", UpdateAction.class);
        actionClasses.put("delete", DeleteAction.class);
    }
    
    public TestAction getTestAction(JsonNode jsonNode, TestActionContext testActionContext) throws SecurityException, NoSuchMethodException, IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException  {
        String actionName = jsonNode.get("action").getTextValue();
        Constructor constructor = actionClasses.get(actionName).getConstructor(JsonNode.class, TestActionContext.class);
        return (TestAction) constructor.newInstance(jsonNode, testActionContext);
    }
}
