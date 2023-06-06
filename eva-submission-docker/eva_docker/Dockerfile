FROM ubuntu:22.04

ENV JAVA_VERSION 8
ENV NXF_VER 21.10.5
ENV VCF_VALIDATOR_VERSION 0.9.4
ENV MONGO_VERSION 4.4
ENV EVA_ACCESSION_VERSION v0.6.21
ENV EVA_PIPELINE_VERSION v2.9.1
ENV EVA_SUBMISSION_VERSION v1.10.6

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
    && apt install -y curl wget maven git vim samtools bcftools tabix bedtools minimap2 rsync  \
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
RUN wget -qO - https://www.mongodb.org/static/pgp/server-${MONGO_VERSION}.asc | apt-key add -
RUN echo "deb [ arch=amd64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/${MONGO_VERSION} multiverse" | tee /etc/apt/sources.list.d/mongodb-org-${MONGO_VERSION}.list
RUN apt update && apt install -y mongodb-database-tools

# install mongodb shell
RUN wget http://archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.0g-2ubuntu4_amd64.deb
RUN dpkg -i ./libssl1.1_1.1.0g-2ubuntu4_amd64.deb
RUN apt install -y mongodb-org-shell

WORKDIR /usr/local/software

COPY eva_docker/mocked_eva_submission_to_xml_V1.1.0.pl /usr/local/software
COPY eva_docker/mocked_load_from_ena_postgres_or_file.pl /usr/local/software

# install vcf_validator
RUN curl -LJo /usr/local/software/vcf_validator https://github.com/EBIvariation/vcf-validator/releases/download/v${VCF_VALIDATOR_VERSION}/vcf_validator_linux \
    && chmod 755 /usr/local/software/vcf_validator

# install vcf_assembly_checker
RUN curl -LJo /usr/local/software/vcf_assembly_checker https://github.com/EBIvariation/vcf-validator/releases/download/v${VCF_VALIDATOR_VERSION}/vcf_assembly_checker_linux \
    && chmod 755 /usr/local/software/vcf_assembly_checker

# setup eva-accession
RUN git clone -b ${EVA_ACCESSION_VERSION} https://github.com/EBIvariation/eva-accession.git
RUN mvn package -s /root/maven-settings.xml -f eva-accession/pom.xml -DskipTests -P docker
RUN cp eva-accession/eva-accession-pipeline/target/eva-accession-pipeline-*-exec.jar /usr/local/software/eva-accession-pipeline.jar
RUN cp eva-accession/eva-accession-clustering/target/eva-accession-clustering-*-exec.jar /usr/local/software/eva-accession-clustering.jar
RUN cp eva-accession/eva-remapping-get-source/target/eva-remapping-get-source-*-exec.jar /usr/local/software/eva-remapping-get-source.jar
RUN cp eva-accession/eva-remapping-ingest/target/eva-remapping-ingest-*-exec.jar /usr/local/software/eva-remapping-ingest.jar

# setup eva-pipeline
RUN git clone -b ${EVA_PIPELINE_VERSION} https://github.com/EBIvariation/eva-pipeline.git
RUN mvn package -s /root/maven-settings.xml -f eva-pipeline/pom.xml -DskipTests -P docker
RUN cp eva-pipeline/target/eva-pipeline-*.jar /usr/local/software/eva-pipeline.jar

# setup eva-submission
RUN git clone -b ${EVA_SUBMISSION_VERSION} https://github.com/EBIvariation/eva-submission.git
RUN python3 -m venv eva-submission/venv
RUN eva-submission/venv/bin/pip3 install --upgrade pip
RUN eva-submission/venv/bin/pip3 install /usr/local/software/eva-submission

ENV PATH="${PATH}:/usr/local/software/eva-submission/venv/bin"