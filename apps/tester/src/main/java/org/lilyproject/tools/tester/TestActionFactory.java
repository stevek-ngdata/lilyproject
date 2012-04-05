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
package org.lilyproject.tools.tester;

import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonNode;

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
