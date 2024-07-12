FROM python:3.9-slim

WORKDIR /api

RUN apt-get update \
    && apt-get install -y --no-install-recommends locales \
    && echo "es_ES.UTF-8 UTF-8" > /etc/locale.gen \
    && locale-gen

RUN set -ex \
    && apt-get install -y --no-install-recommends build-essential libmariadb-dev mariadb-client \
    && pip install --upgrade pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /api/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . /api

ENV LANG es_ES.UTF-8
ENV LANGUAGE es_ES:es
ENV LC_ALL es_ES.UTF-8

ENV FLASK_APP=api
ENV FLASK_DEBUG=0
ENV FLASK_ENV=production

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--timeout", "120", "wsgi:app"]


# docker build -t pfpb_ms-offerm1:v1 .
# docker run -p 5003:8080 pfpb_ms-offerm1:v1
#set MARIADB_CC_INSTALL_DIR=C:\Users\LadHof\Desktop\PB PF\mariadb-connector-c-3.3.7-src\mariadb-connector-c-3.3.7-src