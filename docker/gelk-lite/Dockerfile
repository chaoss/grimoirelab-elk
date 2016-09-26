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

# install minimal dependencies for a python3 box and misc utils
RUN apt-get update && \
    apt-get -y install --no-install-recommends \
        bash locales \
        git git-core \
        tree ccze \
        psmisc \
        python python3 pep8 \
        python3-pip \
        unzip curl wget sudo vim ssh \
        && \
    apt-get clean && \
    find /var/lib/apt/lists -type f -delete

# perceval needs a newer version than Debian 8 - this breaks pip3
RUN pip3 install --upgrade pip
RUN pip3 install --upgrade requests
RUN apt-get -y remove python3-requests

RUN echo "${DEPLOY_USER}    ALL=NOPASSWD: ALL" >> /etc/sudoers

# Configuring and UTF-8 locale
RUN sed -i "s/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/g" /etc/locale.gen && \
    locale-gen "en_US.UTF-8" && update-locale && \
    echo "export LANG='en_US.utf8'" >> ${DEPLOY_USER_DIR}/.bashrc

USER ${DEPLOY_USER}
WORKDIR ${DEPLOY_USER_DIR}

# get GrimoireELK repository
RUN git clone https://github.com/grimoirelab/GrimoireELK

# get Perceval repository
RUN git clone https://github.com/grimoirelab/perceval.git && \
    cd perceval && sudo python3 setup.py install

ADD init-gelk ${DEPLOY_USER_DIR}/

CMD ${DEPLOY_USER_DIR}/init-gelk
