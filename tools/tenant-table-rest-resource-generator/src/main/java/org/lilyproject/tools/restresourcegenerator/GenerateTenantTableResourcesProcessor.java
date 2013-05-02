/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.tools.restresourcegenerator;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.ws.rs.Path;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An annotation processor to automatically generate table and tenant+table variants of some JAX-RS resource
 * classes. This will be done for classes annotated with {@link GenerateTenantAndTableResource}.
 */
@SupportedAnnotationTypes(
        {"org.lilyproject.tools.restresourcegenerator.GenerateTenantAndTableResource",
        "org.lilyproject.tools.restresourcegenerator.GenerateTableResource",
        "org.lilyproject.tools.restresourcegenerator.GenerateTenantResource"})
public class GenerateTenantTableResourcesProcessor extends AbstractProcessor {
    private static Map<String, ResourceClassGenerator> generators = new HashMap<String, ResourceClassGenerator>();
    static {
        generators.put(GenerateTableResource.class.getName(), new TableBasedResourceGenerator());
        generators.put(GenerateTenantResource.class.getName(), new TenantBasedResourceGenerator());
        generators.put(GenerateTenantAndTableResource.class.getName(), new TenantAndTableBasedResourceGenerator());
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (TypeElement annotation : annotations) {
            for (Element element : roundEnv.getElementsAnnotatedWith(annotation)) {
                Path pathAnnotation = element.getAnnotation(Path.class);
                if (pathAnnotation == null) {
                    throw new RuntimeException("A JAX-RS resource with the "
                            + GenerateTenantAndTableResource.class.getSimpleName()
                            + " annotation is missing a javax.ws.rs.Path annotation: " + element.toString());
                }
                String jaxRsPath = pathAnnotation.value();

                Filer filer = processingEnv.getFiler();
                String packageName = element.getEnclosingElement().toString();
                String className = element.getSimpleName().toString();

                generators.get(annotation.getQualifiedName().toString())
                        .generateResourceClass(packageName, className, jaxRsPath, filer);
            }
        }

        return true;
    }

    private static interface ResourceClassGenerator {
        void generateResourceClass(String packageName, String className, String jaxRsPath, Filer filer);
    }

    private static class TableBasedResourceGenerator implements ResourceClassGenerator {
        @Override
        public void generateResourceClass(String packageName, String className, String jaxRsPath, Filer filer) {
            System.out.println("Generating /table/{tableName} variant of JAX-RS class " + packageName + "." + className);
            try {
                String generatedClassName = "TableBased" + className;
                PrintWriter writer = new PrintWriter(filer.createSourceFile(packageName + "." + generatedClassName).openWriter());
                writer.println("package " + packageName + ";");
                writer.println();
                writer.println("import javax.ws.rs.Path;");
                writer.println();
                writer.println("@TableEnabled");
                writer.println("@Path(\"table/{tableName}/" + escapeJavaString(jaxRsPath) + "\")");
                writer.println("public class " + generatedClassName + " extends " + className + " {");
                writer.println("}");
                writer.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class TenantBasedResourceGenerator implements ResourceClassGenerator {
        @Override
        public void generateResourceClass(String packageName, String className, String jaxRsPath, Filer filer) {
            try {
                System.out.println("Generating /tenant/{tenantName} variant of JAX-RS class " + packageName + "." + className);
                String generatedClassName = "TenantBased" + className;
                PrintWriter writer = new PrintWriter(filer.createSourceFile(packageName + "." + generatedClassName).openWriter());
                writer.println("package " + packageName + ";");
                writer.println();
                writer.println("import javax.ws.rs.Path;");
                writer.println();
                writer.println("@TenantEnabled");
                writer.println("@Path(\"tenant/{tenantName}/" + escapeJavaString(jaxRsPath) + "\")");
                writer.println("public class " + generatedClassName + " extends " + className + " {");
                writer.println("}");
                writer.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class TenantAndTableBasedResourceGenerator implements ResourceClassGenerator {
        @Override
        public void generateResourceClass(String packageName, String className, String jaxRsPath, Filer filer) {
            try {
                System.out.println("Generating /tenant/{tenantName}/table/{tableName} variant of JAX-RS class " + packageName + "." + className);
                String generatedClassName = "TableAndTenantBased" + className;
                PrintWriter writer = new PrintWriter(filer.createSourceFile(packageName + "." + generatedClassName).openWriter());
                writer.println("package " + packageName + ";");
                writer.println();
                writer.println("import javax.ws.rs.Path;");
                writer.println();
                writer.println("@TableEnabled");
                writer.println("@TenantEnabled");
                writer.println("@Path(\"tenant/{tenantName}/table/{tableName}/" + escapeJavaString(jaxRsPath) + "\")");
                writer.println("public class " + generatedClassName + " extends " + className + " {");
                writer.println("}");
                writer.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static String escapeJavaString(String input) {
        return input.replaceAll(Pattern.quote("\\"), Matcher.quoteReplacement("\\\\"));
    }
}
