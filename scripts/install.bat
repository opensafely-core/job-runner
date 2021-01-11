@echo off
REM This scripts installs/configures the opensafely service, designed to be
REM installed on the opensafely TPP backend

REM We make some assumtions about paths for the TPP backend
set directory=E:\job-runner
set nssm=C:\nssm-2.24\win64\nssm.exe

REM we require both user name and a password to run, so check
if "%1" == "" GOTO :usage
if "%2" == "" GOTO :usage

REM check the service is installed, and either stop it or install it.
sc query opensafely | find "does not exist" >nul
if %ERRORLEVEL% NEQ 0 ( 
  %nssm% stop opensafely
) else ( 
  %nssm% install opensafely "C:\Program Files\Git\usr\bin\bash" 
)

REM now we know the service definitely exists, configure it

%nssm% set opensafely Application "C:\Program Files\Git\usr\bin\bash"
%nssm% set opensafely AppParameters "%directory%\scripts\run.sh -m jobrunner.service"
%nssm% set opensafely AppDirectory %directory%
%nssm% set opensafely DisplayName "OpenSAFELY job runner"
%nssm% set opensafely Start SERVICE_AUTO_START

REM check docker service exists, because $REASONS
sc query com.docker.service | find "does not exist" >nul
if %ERRORLEVEL% NEQ 0 ( %nssm% set opensafely DependOnService com.docker.service )

%nssm% set opensafely ObjectName %1 %2
%nssm% set opensafely AppThrottle 10000
%nssm% set opensafely AppExit Default Restart
%nssm% set opensafely AppRestartDelay 1000
REM don't send WM_CLOSE or WM_QUIT, but do send Ctrl-C and TerminateProcess
%nssm% set opensafely AppStopMethodSkip 6
%nssm% set opensafely AppStdout %directory%\service.log
%nssm% set opensafely AppStderr %directory%\service.err.log
%nssm% set opensafely AppStdoutCreationDisposition 4
%nssm% set opensafely AppStderrCreationDisposition 4
%nssm% set opensafely AppRotateFiles 1
REM rotate after 1 day or 1Gb (AppRotateBytes is in kb)
%nssm% set opensafely AppRotateSeconds 86400
%nssm% set opensafely AppRotateBytes 1048576

REM we are done, try start the service
%nssm% start opensafely
EXIT /B 0


:usage
echo usage: install.bat USER PASSWORD
EXIT /B 1
