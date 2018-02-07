# Dataflow batch examples
 
## Running examples
 
* Usage: `./make.sh <command> <projectId> <jobClass|jobName|templateName> <templateClient>`
* command:
- runLocal: runs a pipeline job on local
- runCloud: runs a pipeline job in cloud
- deployTemplate: deploys a template to cloud
- runTemplate: creates and run a job from the template in cloud

* example:
- `/> ./make.sh runCloud projectId ChangingDataInDatastore`
 
# prerequisites

* Google Account is set up for a google project to run pipelines there by `gcloud init`
