@echo off
setlocal

if not "%KAURI_HOME%"=="" goto gotKauriHome
echo KAURI_HOME not set!
pause
goto :eof
:gotKauriHome

rem Copyright (c) 1999, 2008 Tanuki Software, Inc.
rem http://www.tanukisoftware.com
rem All rights reserved.
rem
rem This software is the proprietary information of Tanuki Software.
rem You shall use it only in accordance with the terms of the
rem license agreement you entered into with Tanuki Software.
rem http://wrapper.tanukisoftware.org/doc/english/licenseOverview.html
rem
rem Java Service Wrapper general NT service install script.
rem Optimized for use with version 3.3.1 of the Wrapper.
rem

if "%OS%"=="Windows_NT" goto nt
echo This script only works with NT-based versions of Windows.
goto :eof

:nt
rem
rem Find the application home.
rem
rem %~dp0 is location of current script under NT
set _REALPATH=%~dp0
rem this is the place of the wrapper binaries inside the Kauri distribution
set _WRAPPER_BIN_DIR=%KAURI_HOME%\wrapper\bin\

rem Decide on the wrapper binary.
set _WRAPPER_BASE=wrapper
set _WRAPPER_EXE=%_WRAPPER_BIN_DIR%%_WRAPPER_BASE%-windows-x86-32.exe
if exist "%_WRAPPER_EXE%" goto conf
set _WRAPPER_EXE=%_WRAPPER_BIN_DIR%%_WRAPPER_BASE%-windows-x86-64.exe
if exist "%_WRAPPER_EXE%" goto conf
set _WRAPPER_EXE=%_WRAPPER_BIN_DIR%%_WRAPPER_BASE%.exe
if exist "%_WRAPPER_EXE%" goto conf
echo Unable to locate a Wrapper executable using any of the following names:
echo %_WRAPPER_BIN_DIR%%_WRAPPER_BASE%-windows-x86-32.exe
echo %_WRAPPER_BIN_DIR%%_WRAPPER_BASE%-windows-x86-64.exe
echo %_WRAPPER_BIN_DIR%%_WRAPPER_BASE%.exe
pause
goto :eof

rem
rem Find the wrapper.conf
rem
:conf
set _WRAPPER_CONF="%~f1"
if not %_WRAPPER_CONF%=="" goto startup
set _WRAPPER_CONF="%_REALPATH%service-wrapper.conf"

rem
rem Install the Wrapper as an NT service.
rem
:startup
"%_WRAPPER_EXE%" -i %_WRAPPER_CONF%
if not errorlevel 1 goto :eof
pause
