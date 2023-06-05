FROM ubuntu:22.04

ENV JAVA_VERSION 8
ENV NXF_VER 21.10.5
ENV vcf_validator_version 0.9.4

COPY maven-settings.xml /root
COPY eva_docker/submission_config.yml /root/.submission_config.yml

RUN mkdir -p /usr/local/test_eva_submission/data
RUN mkdir -p /usr/local/test_eva_submission/submissions
RUN mkdir -p /usr/local/eva/public_ftp/
RUN mkdir -p /usr/local/eva/datasources/reference_sequences
RUN mkdir -p /usr/local/eva/datasources/vep-cache
RUN mkdir -p /usr/local/software/opencga

ADD eva_docker/ftp /usr/local/eva/ftp/
ADD eva_docker/conf /usr/local/software/opencga/conf

# install utility libs
RUN apt update  \
    && apt install -y curl wget maven git vim samtools bcftools tabix bedtools minimap2  \
    && apt install -y python3-pip python3.10-venv

# install java
RUN apt install -y openjdk-${JAVA_VERSION}-jdk
# Set Java Home
ENV JAVA_HOME /usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

# install nextflow
RUN curl -L "https://github.com/nextflow-io/nextflow/releases/download/v${NXF_VER}/nextflow-${NXF_VER}-all" | bash \
    && mv nextflow /usr/local/bin/

# install mongodb tools
RUN wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | apt-key add -
RUN echo "deb [ arch=amd64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/4.4 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-4.4.list
RUN apt update && apt install -y mongodb-database-tools

# install mongodb shell
RUN wget http://archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.0g-2ubuntu4_amd64.deb
RUN dpkg -i ./libssl1.1_1.1.0g-2ubuntu4_amd64.deb
RUN apt install -y mongodb-org-shell

WORKDIR /usr/local/software

COPY eva_docker/mocked_eva_submission_to_xml_V1.1.0.pl /usr/local/software
COPY eva_docker/mocked_load_from_ena_postgres_or_file.pl /usr/local/software

# install vcf_validator
RUN curl -LJo /usr/local/software/vcf_validator https://github.com/EBIvariation/vcf-validator/releases/download/v${vcf_validator_version}/vcf_validator_linux \
    && chmod 755 /usr/local/software/vcf_validator

# install vcf_assembly_checker
RUN curl -LJo /usr/local/software/vcf_assembly_checker https://github.com/EBIvariation/vcf-validator/releases/download/v${vcf_validator_version}/vcf_assembly_checker_linux \
    && chmod 755 /usr/local/software/vcf_assembly_checker

# setup eva-accession
RUN git clone https://github.com/EBIvariation/eva-accession.git
RUN mvn package -s /root/maven-settings.xml -f eva-accession/pom.xml -DskipTests -P docker
RUN cp eva-accession/eva-accession-pipeline/target/eva-accession-pipeline-*-exec.jar /usr/local/software/eva-accession-pipeline.jar
RUN cp eva-accession/eva-accession-clustering/target/eva-accession-clustering-*-exec.jar /usr/local/software/eva-accession-clustering.jar
RUN cp eva-accession/eva-remapping-get-source/target/eva-remapping-get-source-*-exec.jar /usr/local/software/eva-remapping-get-source.jar
RUN cp eva-accession/eva-remapping-ingest/target/eva-remapping-ingest-*-exec.jar /usr/local/software/eva-remapping-ingest.jar

# setup eva-pipeline
RUN git clone https://github.com/EBIvariation/eva-pipeline.git
RUN mvn package -s /root/maven-settings.xml -f eva-pipeline/pom.xml -DskipTests -P docker
RUN cp eva-pipeline/target/eva-pipeline-*.jar /usr/local/software/eva-pipeline.jar

# setup eva-submission
RUN git clone https://github.com/EBIvariation/eva-submission.git
RUN python3 -m venv eva-submission/venv
RUN eva-submission/venv/bin/pip3 install --upgrade pip
RUN eva-submission/venv/bin/pip3 install /usr/local/software/eva-submission

ENV PATH="${PATH}:/usr/local/software/eva-submission/venv/bin"