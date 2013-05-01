package org.lilyproject.tools.restresourcegenerator;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.ws.rs.Path;
import java.io.PrintWriter;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An annotation processor to automatically generate table and tenant+table variants of some JAX-RS resource
 * classes. This will be done for classes annotated with {@link GenerateTenantAndTableResource}.
 */
@SupportedAnnotationTypes({"org.lilyproject.tools.restresourcegenerator.GenerateTenantAndTableResource"})
public class GenerateTenantTableResourcesProcessor extends AbstractProcessor {
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (TypeElement annotation : annotations) {
            for (Element element : roundEnv.getElementsAnnotatedWith(annotation)) {
                System.out.println("Generate table, tenant and tenant-table variants of JAX-RS class " + element.toString());
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

                generateTableBasedResource(packageName, className, jaxRsPath, filer);
                generateTenantBasedResource(packageName, className, jaxRsPath, filer);
                generateTenantAndTableBasedResource(packageName, className, jaxRsPath, filer);
            }
        }

        return true;
    }

    private void generateTableBasedResource(String packageName, String className, String jaxRsPath, Filer filer) {
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

    private void generateTenantAndTableBasedResource(String packageName, String className, String jaxRsPath, Filer filer) {
        try {
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

    private void generateTenantBasedResource(String packageName, String className, String jaxRsPath, Filer filer) {
        try {
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

    private String escapeJavaString(String input) {
        return input.replaceAll(Pattern.quote("\\"), Matcher.quoteReplacement("\\\\"));
    }
}
