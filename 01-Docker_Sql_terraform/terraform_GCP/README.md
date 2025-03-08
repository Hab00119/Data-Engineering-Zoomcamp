# Terraform GCP Infrastructure Setup

This project uses **Terraform** to provision cloud resources on **Google Cloud Platform (GCP)**, including a Cloud Storage bucket and a BigQuery dataset. The setup allows for authentication via a service account JSON key or environment variables.

---

## 🚀 Getting Started

### 1️⃣ Prerequisites
- **Google Cloud Project**: Ensure you have a GCP project set up.
- **Service Account**: Create a service account with the required permissions.
- **Terraform Installed**: [Install Terraform](https://developer.hashicorp.com/terraform/install).

---

### 2️⃣ Setting Up Authentication

There are two ways to authenticate Terraform with GCP:

#### ✅ Option 1: Export Credentials in Terminal
```sh
export GOOGLE_CREDENTIALS="path_to_creds.json"
echo $GOOGLE_CREDENTIALS  # Verify
```

#### ✅ Option 2: Use `variables.tf`
The `variables.tf` file contains a `credentials` variable that specifies the JSON key path:
```hcl
variable "credentials" {
  description = "Path to the Service Account Credentials"
  default     = "./keys/my-creds.json"
}
```
Modify the path if necessary.

---

## 🔧 Terraform Configuration

### 🔹 `main.tf`
Defines:
- Required providers (`google`).
- Authentication using a service account key.
- A **Cloud Storage bucket** (`google_storage_bucket`).
- A **BigQuery dataset** (`google_bigquery_dataset`).

### 🔹 `variables.tf`
Stores variable definitions:
- `credentials`: Path to the service account JSON key.
- `project`: GCP project ID.
- `region`: Default deployment region.
- `location`: Resource location.
- `gcs_bucket_name`: Name of the Cloud Storage bucket.
- `bq_dataset_name`: Name of the BigQuery dataset.

---

## 🔑 GCP Setup Guide

### 1️⃣ Create a GCP Project
- Visit [Google Cloud Console](https://console.cloud.google.com/) and create a new project.

### 2️⃣ Create a Service Account
- Navigate to **IAM & Admin > Service Accounts**.
- Create a new service account.
- Assign the following roles:
  - **Cloud Storage Admin**
  - **Storage Admin**
  - **BigQuery Admin**
  - **Compute Admin**
- Save the JSON key file.

### 3️⃣ Manage IAM Permissions
- To edit service account permissions:
  - Go to **IAM & Admin > IAM**.
  - Select the service account and modify roles.

- To generate a new JSON key:
  - Go to **IAM & Admin > Service Accounts**.
  - Click on the service account.
  - Select **Manage Keys** → **Create new key**.
  - Download the JSON file.

---

## 📂 Project Structure
```
📂 terraform-gcp
 ├── 📜 main.tf           # Terraform configuration
 ├── 📜 variables.tf      # Variable definitions
 ├── 📜 README.md         # This documentation
 ├── 📂 keys/             # (Optional) Store service account key here
```

---
once authenticated, run
## 📌 Terraform Commands

```sh
terraform init       # Initialize the Terraform project (install providers)
terraform plan       # Preview the changes Terraform will make
terraform apply      # Create the resources (add -auto-approve to skip prompt)
terraform destroy    # Destroy all created resources
terraform fmt        # Format Terraform configuration files
```

---

## 🎯 Next Steps
- Modify `variables.tf` to match your project details.
- Run `terraform apply` to deploy the resources.
- Use `terraform destroy` when cleaning up.

---

### 🔗 References
- [Terraform Docs](https://developer.hashicorp.com/terraform/docs)
- [Google Cloud IAM](https://cloud.google.com/iam/docs)
- [BigQuery](https://cloud.google.com/bigquery/docs)

---
