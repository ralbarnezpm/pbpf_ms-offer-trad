#!/bin/bash

# Salir en caso de cualquier error
set -e

# Actualizar repositorios e instalar dependencias básicas
echo "Actualizando repositorios..."
sudo apt-get update

# Instalar Python y pip
echo "Instalando Python y pip..."
sudo apt-get install -y python3 python3-pip

# Instalar Certbot
echo "Instalando Certbot..."
sudo apt-get install -y certbot python3-certbot-nginx

# Instalar Nginx
echo "Instalando Nginx..."
sudo apt-get install -y nginx

# Agregar repositorio de MariaDB y su llave
#echo "Agregando repositorio de MariaDB..."
#sudo apt-key adv --fetch-keys 'https://mariadb.org/mariadb_release_signing_key.asc'
#sudo add-apt-repository 'deb [arch=amd64] http://mirror.23media.com/mariadb/repo/10.5/ubuntu focal main'

# Instalar MariaDB client y el conector de MariaDB para Python
echo "Instalando MariaDB client y conector..."
sudo apt install python3.12-dev build-essential libmariadb-dev
sudo apt install libmariadb3 libmariadb-dev
sudo apt install mariadb-client-core
sudo apt-get update
sudo apt-get install -y mariadb-client libmariadb-dev

# Instalar dependencias de la aplicación Flask
echo "Instalando dependencias de la aplicación Flask..."
pip3 install gunicorn flask mariadb

# Instalar dependencias adicionales de requirements.txt
echo "Instalando dependencias adicionales de requirements.txt..."
pip3 install -r requirements.txt

# Confirmar las versiones instaladas
echo "Versiones instaladas:"
python3 --version
pip3 --version
nginx -v
certbot --version
mariadb --version

echo "Instalación completada exitosamente."
