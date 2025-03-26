FROM ubuntu:22.04


RUN mkdir -p /usr/local/perl_loader/

# install utility libs
RUN apt update  \
    && apt install -y wget \
    && apt install -y libaio-dev alien build-essential \
    && apt install -y perl libdbi-perl libdbi-perl libdbd-pg-perl libxml-libxml-perl

WORKDIR /usr/local/software
# You can find the database.txt file in the configuration repository (DO NOT COMMIT)
COPY perl_loader_docker/databases.txt /usr/local/perl_loader/databases.txt
# You can download the oracle client from https://download.oracle.com/otn_software/linux/instantclient/1926000/oracle-instantclient19.26-basic-19.26.0.0.0-1.x86_64.rpm
COPY perl_loader_docker/oracle-instantclient19.26-basic-19.26.0.0.0-1.x86_64.rpm /usr/local/software
# You can download the oracle client from https://download.oracle.com/otn_software/linux/instantclient/1926000/oracle-instantclient19.26-devel-19.26.0.0.0-1.x86_64.rpm
COPY perl_loader_docker/oracle-instantclient19.26-devel-19.26.0.0.0-1.x86_64.rpm /usr/local/software
RUN alien --script oracle-instantclient19.26-basic-19.26.0.0.0-1.x86_64.rpm
RUN alien --script oracle-instantclient19.26-devel-19.26.0.0.0-1.x86_64.rpm
RUN dpkg -i oracle-instantclient19.26-devel_19.26.0.0.0-2_amd64.deb
RUN dpkg -i oracle-instantclient19.26-basic_19.26.0.0.0-2_amd64.deb
#
ENV ORACLE_HOME="/usr/lib/oracle/19.26/client64"
ENV PATH="${PATH}:${ORACLE_HOME}/bin"
ENV LD_LIBRARY_PATH="${ORACLE_HOME}/lib"

ENV PERL_MM_USE_DEFAULT=1
RUN cpan DBD::Oracle

WORKDIR /usr/local/perl_loader

COPY perl_loader_docker/modified_load_from_ena_postgres_or_file.pl /usr/local/software

