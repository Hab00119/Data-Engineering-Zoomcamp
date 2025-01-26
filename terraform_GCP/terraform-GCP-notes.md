## HashiCorp Terraform
IAC tool that lets u define cloud and on premise resources in human-readable format

terraform init-initialize the providers
terraform plan-what I am about to do
terraform apply-do what is in the tf files (or terraform apply -auto-approve)
terraform destroy-remove everything defined in the tf files

# GCP
create a project
on the project, create a service account using IAM & Admin
allow the following permissions: cloud storage, storage admin, bigQuery Admin, Compute Admin

To edit permission, return to IAM, click on the service account and edit

to access the service account, return back to service account and edit the account and click manage keys,
create a new JSON key, a JSON file will be downloaded (don't show the file to anyone as it contains you credentials to access the account)


## Installation
[HashiCorp](https://developer.hashicorp.com/terraform/install)

use `terraform fmt` to format code

you can export google credentials from terminal, 
export GOOGLE_CREDENTIALS="path to creds"
echo $GOOGLE_CREDENTIALS


# you can create a variables.tf file to store variables