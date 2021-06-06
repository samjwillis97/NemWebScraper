#!/bin/bash
echo "Docker Container has Started"

declare -p | grep -Ev 'BASHOPTS|BASH_VERSINFO|EUID|PPID|SHELLOPTS|UID' > /container.env

CMD busybox syslogd -C
cron -L 2 -f