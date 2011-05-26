This plugins directory is used when launching Lily during development, that is when launching it using:

cd cr/process/server
./target/lily-server

Plugins are Kauri modules (jars), which are loaded during Lily server startup
due to the <directory> directives in the conf/kauri/wiring.xml
