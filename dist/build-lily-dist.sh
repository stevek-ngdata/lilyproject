set -e

mvn -o org.lilyproject:lily-runtime-plugin:assemble-project-repository
mvn -o org.lilyproject:lily-runtime-plugin:assemble-runtime-repository
mvn -o org.lilyproject:lily-runtime-plugin:assemble-pom-repository

mvn assembly:assembly
