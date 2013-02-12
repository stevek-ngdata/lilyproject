@echo off

rem
rem Kauri Runtime startup script
rem

rem
rem You can use the following environment variables to customize the startup
rem
rem KAURI_CLI_CLASSPATH
rem    additional entries to be added to the classpath
rem
rem KAURI_JAVA_OPTIONS
rem    additional options to be passed to the java executable
rem

if "%JAVA_HOME%"=="" goto javaHomeNotSet

rem This technique for detecting KAURI_HOME has been adapted from ant's startup script

if "%KAURI_HOME%"=="" goto setDefaultKauriHome
goto skipDefaultKauriHome

:setDefaultKauriHome
rem %~dp0 is expanded pathname of the current script under NT
set KAURI_HOME=%~dp0..

:skipDefaultKauriHome



set M2_REPO=%KAURI_HOME%\lib

set LAUNCHER_JAR=%M2_REPO%\org\kauriproject\kauri-runtime-launcher\${project.version}\kauri-runtime-launcher-${project.version}.jar

set CP=%LAUNCHER_JAR%

rem Only add KAURI_CLI_CLASSPATH when it is not empty, to avoid adding the working dir to
rem the classpath by accident.
if "%KAURI_CLI_CLASSPATH%"=="" goto noExtraClassPath
set CP=%CP%;%KAURI_CLI_CLASSPATH%
:noExtraClassPath

set KAURI_LOG_OPTS=-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog -Dorg.apache.commons.logging.simplelog.defaultlog=error

"%JAVA_HOME%\bin\java" -classpath "%CP%" %KAURI_JAVA_OPTIONS% %KAURI_LOG_OPTS% "-Dkauri.launcher.repository=%M2_REPO%" org.kauriproject.launcher.RuntimeCliLauncher %*
goto end

:javaHomeNotSet
echo JAVA_HOME not set!
goto end

:end
if "%_EXIT_ERRORLEVEL%"=="true" exit %ERRORLEVEL%
