# no shebang as on windows
#
# assumes to be invoked in working dir of job-runner checkout
# set -a means all declared variables are exported
set -a
source .env
set +a
export PYTHONPATH="lib"
export ENABLE_PERMISSIONS_WORKAROUND=1
exec "C:\\Program Files\\Python39\\python" "$@"
