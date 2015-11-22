# Copyright (C) 2015 Bitergia
# GPLv3 License

FROM debian:8
MAINTAINER Alvaro del Castillo <acs@bitergia.com>

ENV DEBIAN_FRONTEND noninteractive
ENV DEPLOY_USER bitergia
ENV DEPLOY_USER_DIR /home/${DEPLOY_USER}
ENV SCRIPTS_DIR ${DEPLOY_USER_DIR}/scripts

# Initial user
RUN useradd bitergia --create-home

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
        python-requests \
        make \
        unzip wget \
        && \
    apt-get clean && \
    find /var/lib/apt/lists -type f -delete

USER ${DEPLOY_USER}
WORKDIR ${DEPLOY_USER_DIR}

# get Scout repository
RUN git clone https://github.com/acs/GrimoireELK

ENV container docker
CMD ["/sbin/init"]
