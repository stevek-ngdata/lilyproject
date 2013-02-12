@echo off

if "%JAVA_HOME%"=="" goto javaHomeNotSet

rem This technique for detecting KAURI_HOME has been adapted from ant's startup script

if "%KAURI_HOME%"=="" goto setDefaultKauriHome
goto skipDefaultKauriHome

:setDefaultKauriHome
rem %~dp0 is expanded pathname of the current script under NT
set KAURI_HOME=%~dp0..

:skipDefaultKauriHome



set M2_REPO=%KAURI_HOME%\lib

set CLASSPATH=%M2_REPO%\org\kauriproject\kauri-deploy-repo\${project.version}\kauri-deploy-repo-${project.version}.jar

"%JAVA_HOME%\bin\java" -Dkauri.home="%KAURI_HOME%" org.kauriproject.tools.deployrepo.DeployRepo %*
goto end

:javaHomeNotSet
echo JAVA_HOME not set!
goto end

:end
if "%_EXIT_ERRORLEVEL%"=="true" exit %ERRORLEVEL%
