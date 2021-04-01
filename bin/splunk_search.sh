#!/bin/bash

# Quit if any function returns an non-zero status.
set -eE
set -o pipefail
help() {
  cat << EOF
Usage: $0 kmalloc start_date end_date
EOF
exit 0
}

if [ $# -lt 2 ]; then
  help
fi

if [[ "$1" == "sid" ]]; then
  shift
  sid=$1
  shift
fi

if [[ "$1" == "kmalloc" ]]; then
  shift 
  if [ $# -lt 2 ]; then
    help
  fi
  start_date=$1:0:0:0
  end_date=$1:23:59:59
  query="search process=nhc check_hw_slab_unreclaimable earliest=${start_date} latest=${end_date}"
fi


baseurl="https://splunk.osc.edu:8089/services/search/jobs"

echo "Username: $USER" >&2
username=$USER
#read username
echo "Passsword: " >&2
read -s password

if [[ -z "$sid" ]]; then
  response=`curl -s -k -u $username:$password $baseurl -d search="$query"`
  sid=`echo $response | sed -n 's/.*<sid>\(.*\)<\/sid>.*/\1/p'`
  echo Query: $query
  echo SID: $sid
  echo
else
  curl -k -u $username:$password $baseurl/$sid/results --get -d "output_mode=csv&count=0"
fi

trap "exit 1" TERM
