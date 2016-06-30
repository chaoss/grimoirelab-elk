# Copyright (C) 2016 Bitergia
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
        bash locales \
        git git-core \
        tree ccze \
        psmisc \
        python python3 pep8 \
        python3-requests python3-dateutil python3-bs4 \
        python3-pip python3-dev python3-redis python3-sqlalchemy \
        python-mysqldb \
        python3-cherrypy3 \
        gcc g++ make libmysqlclient-dev mariadb-client \
        unzip curl wget sudo vim ssh \
        && \
    apt-get clean && \
    find /var/lib/apt/lists -type f -delete

# Not available as package in Debian 8 python3-myssqldb
RUN pip3 install mysqlclient
# rq tasks queue
RUN pip3 install rq && pip3 install rq-dashboard==0.3.4
# Bug in 0.3.5: https://github.com/nvie/rq-dashboard/pull/89/files
# In 0.3.5 workers don't appear in the dashboard

# Dask
RUN pip3 install dask[complete]

# perceval needs a newer version than Debian 8 - this breaks pip3
RUN pip3 install --upgrade requests

RUN echo "${DEPLOY_USER}    ALL=NOPASSWD: ALL" >> /etc/sudoers

# Configuring and UTF-8 local needed by rqworker
# 10 days for timeout in our workerts
RUN sed -i "s/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/g" /etc/locale.gen && \
    locale-gen "en_US.UTF-8" && update-locale && \
    echo "export LANG='en_US.utf8'" >> ${DEPLOY_USER_DIR}/.bashrc

USER ${DEPLOY_USER}
WORKDIR ${DEPLOY_USER_DIR}

# get SortingHat repository
RUN git clone https://github.com/MetricsGrimoire/sortinghat.git && \
    cd sortinghat && sudo python3 setup.py install

# get GrimoireELK repository
RUN git clone https://github.com/grimoirelab/GrimoireELK && \
	cd GrimoireELK && git checkout arthur

# get VizGrimoireUtils repository for tools like eclipse_projects
RUN git clone https://github.com/VizGrimoire/VizGrimoireUtils.git

# get Perceval repository
RUN git clone https://github.com/grimoirelab/perceval.git && \
    cd perceval && sudo python3 setup.py install

# get Arthur repository
# RUN git clone https://github.com/grimoirelab/arthur.git && \
#    cd arthur && sudo python3 setup.py install && \
#    chmod 755 bin/*
RUN git clone https://github.com/grimoirelab/arthur.git && \
    cd arthur && sudo python3 setup.py install && \
    chmod 755 bin/*


COPY init-gelk /init-gelk.sh
# ADD init-gelk ${DEPLOY_USER_DIR}/

# ENTRYPOINT [${DEPLOY_USER_DIR}/init-gelk]
ENTRYPOINT ["/init-gelk.sh"]
# CMD [/init-gelk.sh]
