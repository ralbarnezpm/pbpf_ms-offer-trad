provider "google" {
  credentials = file("../proytest-398113-717c29301da0.json")
  project     = "proytest-398113"
  region      = "southamerica-east1"
}

resource "google_project_iam_binding" "cloud_run_deployment" {
  project = "proytest-398113"
  role    = "roles/run.admin"

  members = [
    "serviceAccount:ralbarnez-infra@proytest-398113.iam.gserviceaccount.com",
  ]
}
