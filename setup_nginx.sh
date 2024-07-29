#!/bin/bash

# Este script configura Nginx para servir tu aplicación Flask

# Define el contenido del archivo principal de configuración de Nginx
NGINX_MAIN_CONF="
user www-data;
worker_processes 2;
pid /run/nginx.pid;

events {
    worker_connections 1024;
}

http {
    include /etc/nginx/mime.types;
    default_type application/octet-stream;

    sendfile on;
    keepalive_timeout 65;
    client_max_body_size 16M;

    include /etc/nginx/sites-enabled/*;
}
"

# Sobrescribir el archivo de configuración principal de Nginx
echo "$NGINX_MAIN_CONF" | sudo tee /etc/nginx/nginx.conf > /dev/null

# Define el contenido del archivo de configuración de Nginx para el sitio
NGINX_SITE_CONF="
server {
    listen 80;
    server_name pbpf-traditional-offer-service.pricemaker.io;

    # Redirect all HTTP traffic to HTTPS
    location / {
        return 301 https://\$host\$request_uri;
    }
}

server {
    listen 443 ssl;
    server_name pbpf-traditional-offer-service.pricemaker.io ;

    ssl_certificate /etc/letsencrypt/live/pbpf-traditional-offer-service.pricemaker.io /fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/pbpf-traditional-offer-service.pricemaker.io /privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        
        # Aumentar los tiempos de espera a 2 minutos
        proxy_read_timeout 240s;
        proxy_connect_timeout 240s;
        proxy_send_timeout 240s;
    }
}
"

# Escribir el contenido en el archivo de configuración de Nginx para el sitio
echo "$NGINX_SITE_CONF" | sudo tee /etc/nginx/sites-available/default > /dev/null

# Crear enlace simbólico a sites-enabled
sudo ln -sf /etc/nginx/sites-available/default /etc/nginx/sites-enabled/default

# Probar la configuración de Nginx
sudo nginx -t

# Reiniciar Nginx para aplicar los cambios
sudo systemctl restart nginx

echo "Nginx configurado y reiniciado con éxito."
