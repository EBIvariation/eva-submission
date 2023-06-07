CREATE USER contig_alias_user WITH PASSWORD 'contig_alias_pass';
CREATE DATABASE contig_alias;
-- Connect to the database
\c contig_alias

CREATE ROLE eva;

CREATE SCHEMA eva AUTHORIZATION contig_alias_user;

CREATE SEQUENCE eva.hibernate_sequence
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;

ALTER SEQUENCE eva.hibernate_sequence OWNER TO contig_alias_user;
GRANT ALL ON SEQUENCE eva.hibernate_sequence TO contig_alias_user;

CREATE TABLE eva.assembly (
	insdc_accession varchar(255) NOT NULL,
	is_genbank_refseq_identical bool NOT NULL,
	md5checksum varchar(255) NULL,
	"name" varchar(255) NOT NULL,
	organism varchar(255) NULL,
	refseq varchar(255) NULL,
	taxid int8 NOT NULL,
	trunc512checksum varchar(255) NULL,
	genbank varchar(255) NULL,
	CONSTRAINT assembly_pkey PRIMARY KEY (insdc_accession)
);

ALTER TABLE eva.assembly OWNER TO contig_alias_user;
GRANT ALL ON TABLE eva.assembly TO contig_alias_user;

CREATE TABLE eva.chromosome (
	insdc_accession varchar(255) NOT NULL,
	contig_type varchar(255) NULL,
	ena_sequence_name varchar(255) NULL,
	genbank_sequence_name varchar(255) NULL,
	md5checksum varchar(255) NULL,
	refseq varchar(255) NULL,
	seq_length int8 NULL,
	trunc512checksum varchar(255) NULL,
	ucsc_name varchar(255) NULL,
	assembly_insdc_accession varchar(255) NOT NULL,
	genbank varchar(255) NULL,
	assembly_id int8 NULL,
	CONSTRAINT chromosome_pkey PRIMARY KEY (assembly_insdc_accession, insdc_accession),
	CONSTRAINT chromosome_fkey FOREIGN KEY (assembly_insdc_accession) REFERENCES eva.assembly(insdc_accession)
);

ALTER TABLE eva.chromosome OWNER TO contig_alias_user;
GRANT ALL ON TABLE eva.chromosome TO contig_alias_user;

GRANT ALL ON SCHEMA eva TO contig_alias_user;

ALTER DATABASE contig_alias SET search_path TO eva;
