locals {
  data_lake_bucket = "dtc_de_imdb"
}

variable "project" {
  description = "GCP Project Name"
  default = "dtc-de-course-339115"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "asia-south1"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "imdb_data"
}
