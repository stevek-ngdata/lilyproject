@echo off

rem
rem Kauri Runtime startup script for use during Kauri development
rem  meaning: there is no KAURI_HOME, Kauri artifacts are in dev's local Maven repo
rem

rem You can use the following environment variables to customize the startup
rem
rem KAURI_CLI_CLASSPATH
rem    additional entries to be added to the classpath
rem
rem KAURI_JAVA_ARGS
rem    additional options to be passed to the java executable
rem
rem KAURI_DEBUG_ARGS
rem    can be used to define alternative Java debug arguments
rem
rem KAURI_DEBUG_SUSPEND_ARGS
rem    can be used to define alternative Java debug arguments for suspended mode


rem
rem Disclaimer: this script is based on stuff from Apache Cocoon's 'cocoon.bat' script
rem

:: ----- Verify and Set Required Environment Variables -------------------------

if not "%JAVA_HOME%"=="" goto gotJavaHome
echo JAVA_HOME not set!
goto end
:gotJavaHome

:: ----- Find out home dir of this script --------------------------------------

if not "%KAURI_SRC_HOME%"=="" goto gotKauriSrcHome
rem %~dp0 is expanded pathname of the current script under NT
set KAURI_SRC_HOME=%~dp0
:gotKauriSrcHome

:: ----- Check System Properties -----------------------------------------------

set LAUNCHER_JAR=%KAURI_SRC_HOME%\core\kauri-runtime-launcher\target\kauri-runtime-launcher.jar

IF EXIST %LAUNCHER_JAR% goto launcherJarExists
echo Launcher jar not found at %LAUNCHER_JAR%
echo Please build Kauri first using 'mvn install'
goto end

:launcherJarExists

set CLASSPATH=%LAUNCHER_JAR%

rem Only add KAURI_CLI_CLASSPATH when it is not empty, to avoid adding the working dir to
rem the classpath by accident.
if "%KAURI_CLI_CLASSPATH%"=="" goto noExtraClassPath
set CLASSPATH=%CLASSPATH%;%KAURI_CLI_CLASSPATH%
:noExtraClassPath

if not "%KAURI_DEBUG_ARGS%"=="" goto gotKauriDebugArgs
set KAURI_DEBUG_ARGS=-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005
:gotKauriDebugArgs

if not "%KAURI_DEBUG_SUSPEND_ARGS%"=="" goto gotKauriDebugSuspendArgs
set KAURI_DEBUG_SUSPEND_ARGS=-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005
:gotKauriDebugSuspendArgs

:: ----- Build params to forward to Kauri --------------------------------------
set action=%1

set param=
shift
:cliLoop
if "%1"=="" goto cliLoopEnd
if not "%1"=="" set param=%param% %1
shift
goto cliLoop

:cliLoopEnd

:: ----- Check action ----------------------------------------------------------

if ""%action%"" == """" goto doRun
if ""%action%"" == ""run"" goto doRun
if ""%action%"" == ""debug"" goto doDebug
if ""%action%"" == ""debug-suspend"" goto doDebugSuspend

echo Usage: kauri (action)
echo actions:
echo   run             Run the Kauri Runtime (default)
echo   debug           Run the Kauri Runtime, listed to debugger on port 5005
echo   debug-suspend   Run the Kauri Runtime, suspend for debugger on port 5005
echo:
echo To see help options for Kauri, use kauri run -h
echo To pass options to Kauri, use kauri run {options}
goto end

:doRun

"%JAVA_HOME%/bin/java" %KAURI_JAVA_ARGS% org.kauriproject.launcher.RuntimeCliLauncher %param%
goto end

:doDebug

"%JAVA_HOME%/bin/java" %KAURI_JAVA_ARGS% %KAURI_DEBUG_ARGS% org.kauriproject.launcher.RuntimeCliLauncher %param%
goto end

:doDebugSuspend
echo:
echo Don't forget: you'll need to connect with a debugger for Kauri to start
echo:
"%JAVA_HOME%/bin/java" %KAURI_JAVA_ARGS% %KAURI_DEBUG_SUSPEND_ARGS% org.kauriproject.launcher.RuntimeCliLauncher %param%
goto end

:end
if "%_EXIT_ERRORLEVEL%"=="true" exit %ERRORLEVEL%
