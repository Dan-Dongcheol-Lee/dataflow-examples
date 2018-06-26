# Dataflow batch examples
 
# Setting up Google Account

* Set up a Google Account on a local machine to run & deploy pipelines
- `gcloud init`: select your account and a project

 
## Running examples
 
* Usage: `./make.sh <command> <projectId> <jobClass|jobName|templateName> <templateClient>`
* command:
- runLocal: runs a pipeline job on local
- runCloud: runs a pipeline job in cloud
- deployTemplate: deploys a template to cloud
- runTemplate: creates and run a job from the template in cloud

* example:
- `/> ./make.sh runCloud projectId ChangingDataInDatastore`
 