set -e

mvn -o org.lilyproject:lily-kauri-plugin:assemble-project-repository
mvn -o org.lilyproject:lily-kauri-plugin:assemble-runtime-repository
mvn -o org.lilyproject:lily-kauri-plugin:assemble-pom-repository

mvn assembly:assembly
