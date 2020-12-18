REM usage: install.bat USER PASSWORD
nssm install opensafely "C:\Program Files\Python39\python" -m jobrunner.service
nssm set opensafely AppDirectory E:\job-runner
nssm set opensafely DisplayName "OpenSAFELY job runner"
nssm set opensafely Start SERVICE_AUTO_START
REM Note: use AppEnvironmentExtra to avoid any potential conflicts with registry based AppEnvironment key
nssm set opeensafely AppEnvironmentExtra PYTHONPATH=lib
REM login as user
nssm set opensafely ObjectName %1% %2%
nssm set opensafely DependOnService com.docker.service
nssm set opensafely AppThrottle 10000
nssm set opensafely AppExit Default Restart
nssm set opensafely AppRestartDelay 1000
# don't send WM_CLOSE or WM_QUIT, but do send Ctrl-C and TerminateProcess
nssm set opensafely AppStopMethodSkip 6
nssm set opensafely AppStdout E:\job-runner\service.log
nssm set opensafely AppStderr E:\job-runner\service.err.log
nssm set opensafely AppStdoutCreationDisposition 4
nssm set opensafely AppStderrCreationDisposition 4
nssm set opensafely AppRotateFiles 1
REM rotate after 1 day or 1Gb (AppRotateBytes is in kb)
nssm set opensafely AppRotateSeconds 86400
nssm set opensafely AppRotateBytes 1048576
