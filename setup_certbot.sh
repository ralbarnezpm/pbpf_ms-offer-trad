#!/bin/bash

# Salir en caso de cualquier error
set -e

# Variables
DOMAIN="pbpf-traditional-offer-service.pricemaker.io "
EMAIL="rolandoalbarnez@pricemaker.io"

# Generar el certificado SSL con Certbot
echo "Generando el certificado SSL para $DOMAIN..."
sudo certbot --nginx -d $DOMAIN --non-interactive --agree-tos --email $EMAIL

# Reiniciar Nginx para aplicar los cambios
echo "Reiniciando Nginx..."
sudo systemctl restart nginx

echo "Certificado SSL generado y Nginx reiniciado exitosamente."