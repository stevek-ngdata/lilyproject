package org.lilyproject.client;

import com.google.common.collect.Lists;
import org.lilyproject.repository.api.*;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;

public class TracingRepository {

    public static Repository wrap(Repository repository) {
        TypeManager typeManager = (TypeManager)Proxy.newProxyInstance(TypeManager.class.getClassLoader(),
                new Class[] { TypeManager.class }, new TracingTypeManagerIH(repository.getTypeManager()));
        
        IdGenerator idGenerator = (IdGenerator)Proxy.newProxyInstance(IdGenerator.class.getClassLoader(),
                new Class[] { IdGenerator.class }, new TracingIdGeneratorIH(repository.getIdGenerator()));

        return (Repository)Proxy.newProxyInstance(Repository.class.getClassLoader(),
                new Class[] { Repository.class }, new TracingRepositoryIH(repository, typeManager, idGenerator));
    }

    private static final class TracingRepositoryIH implements InvocationHandler {
        private final Repository delegate;
        private final TypeManager typeManager;
        private final IdGenerator idGenerator;

        private TracingRepositoryIH(Repository delegate, TypeManager typeManager, IdGenerator idGenerator) {
            this.delegate = delegate;
            this.typeManager = typeManager;
            this.idGenerator = idGenerator;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getName().equals("getTypeManager")) {
                return typeManager;
            } else if (method.getName().equals("getIdGenerator")) {
                return idGenerator;
            }

            logMethodCall(method, args);

            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }
    }
    
    private static void logMethodCall(Method method, Object[] args) {        
        StringBuilder builder = new StringBuilder();
        builder.append(method.getClass().getSimpleName()).append(".").append(method.getName());
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                builder.append(i > 0 ? ", " : " ");
                builder.append("arg").append(i).append(" = ");
                if (args[i] != null && args[i].getClass().isArray()) {
                    Object[] values = (Object[])args[i];
                    builder.append("[");
                    for (int j = 0; j < values.length; j++) {
                        if (j > 0)
                            builder.append(", ");
                        builder.append(values[i]);
                    }
                    builder.append("]");
                } else {
                    builder.append(args[i]);
                }
            }
        }
        System.out.println("===== " + builder.toString());
    }
    
    private static final class TracingTypeManagerIH implements InvocationHandler {
        private final TypeManager delegate;
        
        private TracingTypeManagerIH(TypeManager delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }
    }

    private static final class TracingIdGeneratorIH implements InvocationHandler {
        private final IdGenerator delegate;

        private TracingIdGeneratorIH(IdGenerator delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }
    }
}
