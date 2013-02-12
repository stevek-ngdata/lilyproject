@echo off

rem
rem Bat file to invoke mvn archetype:generate
rem

if "%KAURI_HOME%"=="" goto setDefaultKauriHome
goto skipDefaultKauriHome

:setDefaultKauriHome
rem %~dp0 is expanded pathname of the current script under NT
set KAURI_HOME=%~dp0..

:skipDefaultKauriHome

mvn -q archetype:generate -DarchetypeCatalog=file:///%KAURI_HOME%\lib
