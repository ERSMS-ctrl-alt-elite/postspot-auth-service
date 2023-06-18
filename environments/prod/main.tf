/* -------------------------------------------------------------------------- */
/*                          Define Cloud Run service                          */
/* -------------------------------------------------------------------------- */
resource "google_cloud_run_v2_service" "default" {
  name     = "${var.service_name}-${var.environment}"
  project  = var.gcp_project_id
  location = var.gcp_region 
  ingress  = "INGRESS_TRAFFIC_ALL"

  template {
    containers {
      image = var.latest_image_tag

      env {
        name = "CLIENT_ID" 
        value_source {
          secret_key_ref {
            secret = "CLIENT_ID"
            version = "1"
          }
        }
      }
    }
  }
}

/* -------------------------------------------------------------------------- */
/*                  Allow public access for Cloud Run service                 */
/* -------------------------------------------------------------------------- */
data "google_iam_policy" "noauth" {
  binding {
    role = "roles/run.invoker"
    members = [
      "allUsers",
    ]
  }
}

resource "google_cloud_run_service_iam_policy" "noauth" {
  location = google_cloud_run_v2_service.default.location
  project  = google_cloud_run_v2_service.default.project
  service  = google_cloud_run_v2_service.default.name

  policy_data = data.google_iam_policy.noauth.policy_data
}