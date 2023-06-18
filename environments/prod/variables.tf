variable "gcp_project_id" {
  type = string
}

variable "gcp_region" {
  type    = string
  default = "europe-central2"
}

variable "environment" {
  type = string
}

variable "app_name" {
  type = string
  default = "postspot"
}

variable "service_name" {
  type = string
  default = "notification-service"
}

variable "latest_image_tag" {
  type = string
}