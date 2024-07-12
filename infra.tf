# provider "google" {
#   credentials = file("path/to/your/credentials.json")
#   project     = "your-project-id"
#   region      = "southamerica-east1"
# }

# # Crear servicios de Cloud Run
# resource "google_cloud_run_service" "service1" {
#   name     = "nombre-del-servicio1"
#   location = "southamerica-east1"

#   template {
#     spec {
#       containers {
#         image = "gcr.io/proytest-398113/api1-pb-pf:v1"
#       }
#     }

#     metadata {
#       annotations = {
#         "autoscaling.knative.dev/minScale" = "2"
#       }
#     }
#   }
# }

# resource "google_cloud_run_service" "service2" {
#   name     = "nombre-del-servicio2"
#   location = "southamerica-east1"

#   template {
#     spec {
#       containers {
#         image = "gcr.io/proytest-398113/api2-pb-pf:v1" # Cambia a la imagen de tu segundo servicio
#       }
#     }

#     metadata {
#       annotations = {
#         "autoscaling.knative.dev/minScale" = "2"
#       }
#     }
#   }
# }

# # Crear balanceador de carga HTTP(S)
# resource "google_compute_managed_region_backend_service" "backend_service" {
#   name = "backend-service"
#   region = "southamerica-east1"

#   backend {
#     default {
#       backend = {
#         service_name = google_cloud_run_service.service1.name
#         service = "cloudrun.googleapis.com"
#       }
#     }

#     route_action {
#       url_map_name = google_compute_url_map.url_map.name
#     }
#   }

#   protocol = "HTTP"
#   timeout = "15s"
# }

# # Crear el mapa de URL
# resource "google_compute_url_map" "url_map" {
#   name = "url-map"
# }

# resource "google_compute_path_matcher" "service1_path" {
#   name    = "service1-matcher"
#   default = false
#   region  = "southamerica-east1"
#   url_map = google_compute_url_map.url_map.name

#   route_rules {
#     description = "Service 1"
#     match_rules {
#       prefix_match = "/service1" # Ruta para service1
#     }

#     route_action {
#       timeout = "15s"
#       service = google_cloud_run_service.service1.name
#       cors_policy {
#         allow_origin = ["*"]
#         allow_methods = ["GET", "POST", "OPTIONS"]
#         allow_headers = ["*"]
#         max_age = 3600
#       }
#     }
#   }
# }

# resource "google_compute_path_matcher" "service2_path" {
#   name    = "service2-matcher"
#   default = false
#   region  = "southamerica-east1"
#   url_map = google_compute_url_map.url_map.name

#   route_rules {
#     description = "Service 2"
#     match_rules {
#       prefix_match = "/service2" # Ruta para service2
#     }

#     route_action {
#       timeout = "15s"
#       service = google_cloud_run_service.service2.name
#       cors_policy {
#         allow_origin = ["*"]
#         allow_methods = ["GET", "POST", "OPTIONS"]
#         allow_headers = ["*"]
#         max_age = 3600
#       }
#     }
#   }
# }

# # Crear reglas de reenvío
# resource "google_compute_target_http_proxy" "http_proxy" {
#   name        = "http-proxy"
#   url_map     = google_compute_url_map.url_map.id
# }

# resource "google_compute_global_forwarding_rule" "forwarding_rule" {
#   name        = "forwarding-rule"
#   target      = google_compute_target_http_proxy.http_proxy.id
#   port_range  = "80"
# }