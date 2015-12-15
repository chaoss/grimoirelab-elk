# Copyright (C) 2015 Bitergia
# GPLv3 License

FROM debian:8
MAINTAINER Alvaro del Castillo <acs@bitergia.com>

ENV DEBIAN_FRONTEND noninteractive
ENV DEPLOY_USER bitergia
ENV DEPLOY_USER_DIR /home/${DEPLOY_USER}
ENV SCRIPTS_DIR ${DEPLOY_USER_DIR}/scripts

# Initial user
RUN useradd bitergia --create-home --shell /bin/bash

# Helper scripts
RUN mkdir ${DEPLOY_USER_DIR}/scripts

# install dependencies
RUN apt-get update && \
    apt-get -y install --no-install-recommends \
        bash \
        git git-core \
        tree ccze \
        psmisc \
        python python3 pep8 \
        python3-requests python3-dateutil python3-bs4 \
        python3-pip python3-dev \
        python3-redis redis-server \
        gcc make libmysqlclient-dev \
        unzip wget sudo vim ssh \
        && \
    apt-get clean && \
    find /var/lib/apt/lists -type f -delete

# Not available as package in Debian 8 python3-myssqldb
RUN pip3 install mysqlclient

RUN echo "${DEPLOY_USER}    ALL=NOPASSWD: ALL" >> /etc/sudoers

USER ${DEPLOY_USER}
WORKDIR ${DEPLOY_USER_DIR}

# get Scout repository
RUN git clone https://github.com/acs/GrimoireELK

ENV container docker
CMD sudo service redis-server start && sudo tail -f /var/log/redis/*
