FROM postgres:9.6

COPY accession_jt.sql /docker-entrypoint-initdb.d/
COPY variant_jt.sql /docker-entrypoint-initdb.d/
COPY metadata.sql /docker-entrypoint-initdb.d/
COPY contig_alias.sql /docker-entrypoint-initdb.d/