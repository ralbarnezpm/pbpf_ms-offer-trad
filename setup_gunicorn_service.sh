#!/bin/bash

# Archivo de configuración del servicio
SERVICE_FILE="/etc/systemd/system/gunicorn.service"

# Contenido del archivo de servicio
SERVICE_CONTENT="[Unit]
Description=Gunicorn instance to serve Flask app
After=network.target

[Service]
User=ubuntu
Group=www-data
WorkingDirectory=/home/ubuntu/pbstd-soprole
Environment=\"PATH=/home/ubuntu/pbstd-soprole/env/bin\"
ExecStart=/home/ubuntu/pbstd-soprole/env/bin/gunicorn --workers 2 --bind 127.0.0.1:8000 --timeout 180 wsgi:app

[Install]
WantedBy=multi-user.target
"

# Escribir el contenido en el archivo de servicio
echo "$SERVICE_CONTENT" | sudo tee "$SERVICE_FILE" > /dev/null

# Recargar systemd para que reconozca el nuevo servicio
sudo systemctl daemon-reload

# Habilitar y comenzar el servicio
sudo systemctl enable gunicorn
sudo systemctl start gunicorn

echo "Servicio de Gunicorn configurado y activado correctamente."