#!/bin/sh

# Copyright 2016-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# Set default values
OS=""
VER=""
SERVICE_NAME="sg_accel"
# Determine the absolute path of the installation directory
# $( dirname "$0" ) get the directory containing this script
# if we are already in the containing directory we will get '.'
# && pwd will prepend the current directory if the path is relative
# this will result in an absolute path
# Note this line is evaluated not executed in the current shell so
# the current directory is not changed by the 'cd' command
SCRIPT_DIR="$(cd "$(dirname "$0")" >/dev/null && pwd)"
INSTALL_DIR="$(dirname "${SCRIPT_DIR}")"
SRCCFGDIR=${INSTALL_DIR}/examples
SRCCFG=basic_sg_accel_config.json
RUNAS_TEMPLATE_VAR=sg_accel
PIDFILE_TEMPLATE_VAR=/var/run/sg-accel.pid
RUNBASE_TEMPLATE_VAR=/home/sg_accel
GATEWAY_TEMPLATE_VAR=${INSTALL_DIR}/bin/sg_accel
CONFIG_TEMPLATE_VAR=${RUNBASE_TEMPLATE_VAR}/sg_accel.json
LOGS_TEMPLATE_VAR=${RUNBASE_TEMPLATE_VAR}/logs
SERVICE_CMD_ONLY=false

usage() {
  echo "This script upgrades an init service to run a sg_accel instance."
}

ostype() {
  ARCH=$(uname -m | sed 's/x86_//;s/i[3-6]86/32/')

  if [ -f /etc/lsb-release ]; then
    OS=$(lsb_release -si)
    VER=$(lsb_release -sr)
  elif [ -f /etc/debian_version ]; then
    OS=Debian # XXX or Ubuntu??
    VER=$(cat /etc/debian_version)
  elif [ -f /etc/redhat-release ]; then
    OS=RedHat
    VER=$(cat /etc/redhat-release | sed s/.*release\ // | sed s/\ .*//)
  elif [ -f /etc/system-release ]; then
    OS=RedHat
    VER=5.0
  else
    OS=$(uname -s)
    VER=$(uname -r)
  fi

  OS_MAJOR_VERSION=$(echo $VER | sed 's/\..*$//')
  OS_MINOR_VERSION=$(echo $VER | sed s/[0-9]*\.//)
}

#
#script starts here
#

#Figure out the OS type of the current system
ostype

#If the OS is MAC OSX, set the default user account home path to /Users/sg_accel
if [ "$OS" = "Darwin" ]; then
  RUNBASE_TEMPLATE_VAR=/Users/sg_accel
  CONFIG_TEMPLATE_VAR=${RUNBASE_TEMPLATE_VAR}/sg_accel.json
  LOGS_TEMPLATE_VAR=${RUNBASE_TEMPLATE_VAR}/logs
fi

# Make sure we are running with root privilages
if [ $(id -u) != 0 ]; then
  echo "This script should be run as root." >/dev/stderr
  exit 1
fi

#Install the service for the specific platform
case $OS in
Debian)
  case 1:${OS_MAJOR_VERSION:--} in
  $((OS_MAJOR_VERSION >= 8))*)
    systemctl stop ${SERVICE_NAME}
    systemctl start ${SERVICE_NAME}
    ;;
  esac
  ;;
Ubuntu)
  case 1:${OS_MAJOR_VERSION:--} in
  $((OS_MAJOR_VERSION >= 16))*)
    systemctl stop ${SERVICE_NAME}
    systemctl start ${SERVICE_NAME}
    ;;
  $((OS_MAJOR_VERSION >= 12))*)
    service ${SERVICE_NAME} stop
    service ${SERVICE_NAME} start
    ;;
  *)
    echo "ERROR: Unsupported Ubuntu Version \"$VER\""
    usage
    exit 1
    ;;
  esac
  ;;
RedHat* | CentOS | OracleServer)
  case $OS_MAJOR_VERSION in
  5)
    PATH=/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin
    service ${SERVICE_NAME} stop
    service ${SERVICE_NAME} start
    ;;
  6)
    initctl stop ${SERVICE_NAME}
    initctl start ${SERVICE_NAME}
    ;;
  7)
    systemctl stop ${SERVICE_NAME}
    systemctl start ${SERVICE_NAME}
    ;;
  *)
    echo "ERROR: Unsupported RedHat/CentOS Version \"$VER\""
    usage
    exit 1
    ;;
  esac
  ;;
Darwin)
  launchctl unload /Library/LaunchDaemons/com.couchbase.mobile.sg_accel.plist
  launchctl load /Library/LaunchDaemons/com.couchbase.mobile.sg_accel.plist
  ;;
*)
  echo "ERROR: unknown OS \"$OS\""
  usage
  exit 1
  ;;
esac