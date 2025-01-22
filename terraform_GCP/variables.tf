variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my-creds.json"
}

variable "project" {
  description = "Project"
  default     = "terrafoam-demo"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo-dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "terrafoam-demo-terra-bucket"

}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"

}