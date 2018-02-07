#!/usr/bin/env bash

workspace=$(cd $(dirname $0) && pwd)


build() {
  ./gradlew build
  if [ $? -ne 0 ]; then
    echo "ERROR during build"
    exit 1
  fi
}

runLocal() {
  local projectId=$2
  local jobClass=$3
  local timestamp=`date +%Y%m%d%H%M%S`
  local jobName="${jobClass//./-}-${timestamp}"

  echo
  echo "Running ${jobName}"

  ${workspace}/gradlew runJob -PjobClass="${jobClass}" -Poptions="
    --project=${projectId}
    --runner=DirectRunner
    --jobName=${jobName}
    --tempLocation=gs://${projectId}.appspot.com/dataflow-examples/temp
    "
}

runCloud() {
  local projectId=$2
  local jobClass=$3
  local timestamp=`date +%Y%m%d%H%M%S`
  local jobName="${jobClass//./-}-${timestamp}"

  echo
  echo "Running ${jobName}"

  ${workspace}/gradlew runJob -PjobClass="${jobClass}" -Poptions="
    --project=${projectId}
    --runner=DataflowRunner
    --jobName=${jobName}
    --stagingLocation=gs://${projectId}.appspot.com/dataflow-examples/staging
    --tempLocation=gs://${projectId}.appspot.com/dataflow-examples/temp
    "
}

deployTemplate() {
  local projectId=$2
  local jobClass=$3

  echo
  echo "Deploying ${jobName}"

  ${workspace}/gradlew runJob -PjobClass="${jobClass}" -Poptions="
    --project=${projectId}
    --runner=DataflowRunner
    --templateLocation=gs://${projectId}.appspot.com/dataflow-examples/template/${jobClass}
    --stagingLocation=gs://${projectId}.appspot.com/dataflow-examples/staging
    --tempLocation=gs://${projectId}.appspot.com/dataflow-examples/temp
    "
}

runTemplate() {
  local projectId=$2
  local templateName=$3
  local jobClass=$4
  local timestamp=`date +%Y%m%d%H%M%S`
  local jobName="${templateName//./-}-${timestamp}"

  echo
  echo "Creating ${jobName}"

  ${workspace}/gradlew runJob -PjobClass="${jobClass}" -Poptions="
    --project=${projectId}
    --jobName=${jobName}
    --templateLocation=gs://${projectId}.appspot.com/dataflow-examples/template/${templateName}
    --stagingLocation=gs://${projectId}.appspot.com/dataflow-examples/staging
    --tempLocation=gs://${projectId}.appspot.com/dataflow-examples/temp
    "
}

####################
# Execution part
####################

command="help"
if [ "$#" -ge 1 ]; then
    command=$1
fi

if [ "$#" -lt 3 ]; then
    command="help"
fi

cat << EOF
-----------------------------------------------------
 * workspace: ${workspace}
 * command: ${command}
-----------------------------------------------------
EOF

build

case ${command} in
    build|runLocal|runCloud|deployTemplate|runTemplate)
        ${command} "$@"
        ;;
    *)
cat << EOF

 * Usage: ./make.sh <command> <projectId> <jobClass|jobName|templateName>
  * command:
   - runLocal: runs a pipeline job on local
   - runCloud: runs a pipeline job in cloud
   - deployTemplate: deploys a template to cloud
   - runTemplate: creates and run a job from the template in cloud

 * example:
  - /> ./make.sh runCloud projectId ChangingDataInDatastore

EOF
        ;;
esac
