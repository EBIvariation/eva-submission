CREATE USER metadata_user WITH PASSWORD 'metadata_pass';
CREATE DATABASE metadata;
-- Connect to the database
\c metadata

CREATE ROLE evapro;

-----------------------------------------eva_progress_tracker-----------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

CREATE SCHEMA eva_progress_tracker AUTHORIZATION metadata_user;

CREATE TABLE eva_progress_tracker.clustering_release_tracker (
	taxonomy int4 NOT NULL,
	scientific_name text NOT NULL,
	assembly_accession text NOT NULL,
	release_version int8 NOT NULL,
	sources text NOT NULL,
	clustering_status text NULL,
	clustering_start timestamp NULL,
	clustering_end timestamp NULL,
	should_be_clustered bool NULL,
	fasta_path text NULL,
	report_path text NULL,
	tempmongo_instance text NULL,
	should_be_released bool NULL,
	num_rs_to_release int8 NULL,
	total_num_variants int8 NULL,
	release_folder_name text NULL,
	release_status text NULL,
	CONSTRAINT clustering_release_tracker_pkey PRIMARY KEY (taxonomy, assembly_accession, release_version)
);

ALTER TABLE eva_progress_tracker.clustering_release_tracker OWNER TO metadata_user;
GRANT ALL ON TABLE eva_progress_tracker.clustering_release_tracker TO metadata_user;

CREATE TABLE eva_progress_tracker.remapping_tracker (
	"source" text NOT NULL,
	taxonomy int4 NOT NULL,
	scientific_name text NULL,
	origin_assembly_accession text NOT NULL,
	num_studies int4 NOT NULL,
	num_ss_ids int8 NOT NULL,
	release_version int4 NOT NULL,
	assembly_accession text NULL,
	remapping_report_time timestamp NULL DEFAULT now(),
	remapping_status text NULL,
	remapping_start timestamp NULL,
	remapping_end timestamp NULL,
	remapping_version text NULL,
	num_ss_extracted int4 NULL,
	num_ss_remapped int4 NULL,
	num_ss_ingested int4 NULL,
	study_accessions _text NULL,
	CONSTRAINT remapping_tracker_pkey PRIMARY KEY (source, taxonomy, origin_assembly_accession, release_version)
);

ALTER TABLE eva_progress_tracker.remapping_tracker OWNER TO metadata_user;
GRANT ALL ON TABLE eva_progress_tracker.remapping_tracker TO metadata_user;

GRANT ALL ON SCHEMA eva_progress_tracker TO metadata_user;



-----------------------------------------eva_pro------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

CREATE SCHEMA evapro AUTHORIZATION metadata_user;

CREATE TABLE evapro.count_stats (
	id bigserial NOT NULL,
	count int8 NOT NULL,
	identifier json NOT NULL,
	metric varchar(255) NOT NULL,
	process varchar(255) NOT NULL,
	"timestamp" timestamp NOT NULL DEFAULT now(),
	CONSTRAINT count_stats_pkey PRIMARY KEY (id)
);

ALTER TABLE evapro.count_stats OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.count_stats TO metadata_user;

CREATE TABLE evapro.supported_assembly_tracker (
	taxonomy_id int4 NOT NULL,
	"source" varchar(50) NOT NULL,
	assembly_id varchar(25) NOT NULL,
	"current" bool NOT NULL,
	start_date date NOT NULL DEFAULT CURRENT_DATE,
	end_date date NOT NULL DEFAULT 'infinity'::date
);

ALTER TABLE evapro.supported_assembly_tracker OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.supported_assembly_tracker TO metadata_user;

CREATE TABLE evapro.taxonomy (
	taxonomy_id int4 NOT NULL,
	common_name varchar(45) NULL DEFAULT NULL::character varying,
	scientific_name varchar(45) NOT NULL DEFAULT NULL::character varying,
	taxonomy_code varchar(100) NULL,
	eva_name varchar(40) NULL,
	CONSTRAINT taxonomy_pkey PRIMARY KEY (taxonomy_id)
);

ALTER TABLE evapro.taxonomy OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.taxonomy TO metadata_user;

CREATE SEQUENCE evapro.assembly_set_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;

ALTER SEQUENCE evapro.assembly_set_id_seq OWNER TO metadata_user;
GRANT ALL ON SEQUENCE evapro.assembly_set_id_seq TO metadata_user;

CREATE TABLE evapro.assembly_set (
	assembly_set_id int4 NOT NULL DEFAULT nextval('evapro.assembly_set_id_seq'::regclass),
	taxonomy_id int4 NOT NULL,
	assembly_name varchar(64) NULL,
	assembly_code varchar(64) NULL,
	CONSTRAINT assembly_set_taxonomy_id_assembly_name_key UNIQUE (taxonomy_id, assembly_name),
	CONSTRAINT assembly_sid_pkey PRIMARY KEY (assembly_set_id)
);

ALTER TABLE evapro.assembly_set OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.assembly_set TO metadata_user;

ALTER TABLE evapro.assembly_set ADD CONSTRAINT assembly_taxonomy_id_fkey FOREIGN KEY (taxonomy_id) REFERENCES evapro.taxonomy(taxonomy_id);

CREATE TABLE evapro.accessioned_assembly (
	assembly_set_id int4 NOT NULL,
	assembly_accession varchar(25) NOT NULL,
	assembly_chain varchar(25) NOT NULL,
	assembly_version int4 NOT NULL,
	CONSTRAINT assembly_set_id_accession_pk PRIMARY KEY (assembly_set_id, assembly_accession)
);

ALTER TABLE evapro.accessioned_assembly OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.accessioned_assembly TO metadata_user;

ALTER TABLE evapro.accessioned_assembly ADD CONSTRAINT acc_assembly_sid_fk FOREIGN KEY (assembly_set_id) REFERENCES evapro.assembly_set(assembly_set_id);

CREATE TABLE evapro.custom_assembly (
	assembly_set_id int4 NOT NULL,
	assembly_location varchar(250) NOT NULL,
	assembly_file_name varchar(250) NOT NULL,
	CONSTRAINT assembly_loc_file_pkey PRIMARY KEY (assembly_location, assembly_file_name),
	CONSTRAINT custom_assembly_assembly_set_id_key UNIQUE (assembly_set_id)
);

ALTER TABLE evapro.custom_assembly OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.custom_assembly TO metadata_user;

ALTER TABLE evapro.custom_assembly ADD CONSTRAINT cust_assembly_sid_fkey FOREIGN KEY (assembly_set_id) REFERENCES evapro.assembly_set(assembly_set_id);

CREATE TABLE evapro.assembly_accessioning_store_status (
	assembly_accession varchar(25) NOT NULL,
	assembly_in_accessioning_store bool NULL,
	CONSTRAINT assembly_accessioning_store_status_assembly_accession_key UNIQUE (assembly_accession)
);

ALTER TABLE evapro.assembly_accessioning_store_status OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.assembly_accessioning_store_status TO metadata_user;

CREATE OR REPLACE VIEW evapro.assembly
AS SELECT accessioned_assembly.assembly_accession,
    accessioned_assembly.assembly_chain,
    accessioned_assembly.assembly_version,
    assembly_set.assembly_set_id,
    assembly_set.assembly_name,
    assembly_set.assembly_code,
    assembly_set.taxonomy_id,
    custom_assembly.assembly_location,
    custom_assembly.assembly_file_name AS assembly_filename,
    assembly_accessioning_store_status.assembly_in_accessioning_store
   FROM evapro.assembly_set
     LEFT JOIN evapro.accessioned_assembly USING (assembly_set_id)
     LEFT JOIN evapro.custom_assembly USING (assembly_set_id)
     LEFT JOIN evapro.assembly_accessioning_store_status USING (assembly_accession);

ALTER TABLE evapro.assembly OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.assembly TO metadata_user;


CREATE TABLE evapro.analysis (
	analysis_accession varchar(45) NOT NULL,
	title varchar(1000) NOT NULL,
	alias varchar(1000) NOT NULL,
	description varchar(12000) NULL DEFAULT NULL::character varying,
	center_name varchar(500) NULL DEFAULT NULL::character varying,
	"date" timestamp NULL,
	vcf_reference varchar(250) NULL,
	vcf_reference_accession varchar(25) NULL,
	hidden_in_eva int4 NULL DEFAULT 0,
	assembly_set_id int4 NULL,
	CONSTRAINT analysis_pkey PRIMARY KEY (analysis_accession)
);

ALTER TABLE evapro.analysis OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.analysis TO metadata_user;

ALTER TABLE evapro.analysis ADD CONSTRAINT analysis_assembly_set_id_fkey FOREIGN KEY (assembly_set_id) REFERENCES evapro.assembly_set(assembly_set_id);


CREATE TABLE evapro.file_class_cv (
	file_class_id serial4 NOT NULL,
	file_class varchar(45) NOT NULL,
	CONSTRAINT file_class_cv_pkey PRIMARY KEY (file_class_id),
	CONSTRAINT uniq_file_class UNIQUE (file_class)
);

ALTER TABLE evapro.file_class_cv OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.file_class_cv TO metadata_user;

CREATE TABLE evapro.file (
	file_id serial4 NOT NULL,
	ena_submission_file_id varchar(45) NULL DEFAULT NULL::character varying,
	filename varchar(250) NOT NULL,
	file_md5 varchar(250) NOT NULL,
	file_location varchar(250) NULL DEFAULT NULL::character varying,
	file_type varchar(250) NOT NULL,
	file_class varchar(250) NOT NULL,
	file_version int4 NOT NULL,
	is_current int2 NOT NULL,
	ftp_file varchar(250) NULL DEFAULT NULL::character varying,
	mongo_load_status int2 NOT NULL DEFAULT 0,
	eva_submission_file_id varchar(15) NULL,
	CONSTRAINT file_pkey PRIMARY KEY (file_id)
);
CREATE INDEX file_filename_idx ON evapro.file USING btree (filename);
CREATE UNIQUE INDEX file_ids_name_idx ON evapro.file USING btree (file_id, ena_submission_file_id, filename);

CREATE OR REPLACE FUNCTION evapro.insert_eva_submission_file_id()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
	begin
	NEW.eva_submission_file_id = 'EVAF'||to_char(NEW.file_id,'FM00000000');
	return NEW;
	END;
	$function$
;

ALTER FUNCTION evapro.insert_eva_submission_file_id() OWNER TO metadata_user;
GRANT ALL ON FUNCTION evapro.insert_eva_submission_file_id() TO metadata_user;
-- Table Triggers
create trigger eva_sub_file_id_insert before insert or update on evapro.file for each row execute procedure evapro.insert_eva_submission_file_id();

ALTER TABLE evapro.file OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.file TO metadata_user;

ALTER TABLE evapro.file ADD CONSTRAINT file_fileclass_fk FOREIGN KEY (file_class) REFERENCES evapro.file_class_cv(file_class) MATCH FULL;


CREATE TABLE evapro.browsable_file (
	file_id int4 NOT NULL,
	ena_submission_file_id varchar(45) NOT NULL,
	filename varchar(250) NOT NULL,
	loaded bool NOT NULL DEFAULT false,
	eva_release varchar(50) NOT NULL DEFAULT 'Unreleased'::character varying,
	deleted bool NOT NULL DEFAULT false,
	eva_release_deleted varchar(50) NOT NULL DEFAULT 'None'::character varying,
	project_accession varchar(25) NULL,
	loaded_assembly varchar(500) NULL,
	assembly_set_id int4 NULL,
	CONSTRAINT browsable_file_proto_pk PRIMARY KEY (file_id, loaded, eva_release)
);
CREATE INDEX browsablefile_filename_ids ON evapro.browsable_file USING btree (filename);
CREATE UNIQUE INDEX project_file_release_unique_idx ON evapro.browsable_file USING btree (project_accession, filename, eva_release);

ALTER TABLE evapro.browsable_file OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.browsable_file TO metadata_user;

ALTER TABLE evapro.browsable_file ADD CONSTRAINT browsable_file_pag_prototype_assembly_set_id_fkey FOREIGN KEY (assembly_set_id) REFERENCES evapro.assembly_set(assembly_set_id);
ALTER TABLE evapro.browsable_file ADD CONSTRAINT browsable_file_pag_prototype_file_id_fkey FOREIGN KEY (file_id) REFERENCES evapro.file(file_id);



CREATE TABLE evapro.analysis_file (
	analysis_accession varchar(45) NOT NULL,
	file_id int4 NOT NULL,
	CONSTRAINT analysis_file_pkey PRIMARY KEY (analysis_accession, file_id)
);

CREATE INDEX analfile_analacc_idx ON evapro.analysis_file USING btree (analysis_accession);
CREATE UNIQUE INDEX analfile_analfile_idx ON evapro.analysis_file USING btree (analysis_accession, file_id);
CREATE INDEX analfile_fileid_idx ON evapro.analysis_file USING btree (file_id);
CREATE INDEX analysis_file_analysis_accession_idx ON evapro.analysis_file USING btree (analysis_accession);
CREATE INDEX analysis_file_file_id_idx ON evapro.analysis_file USING btree (file_id);

ALTER TABLE evapro.analysis_file OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.analysis_file TO metadata_user;

ALTER TABLE evapro.analysis_file ADD CONSTRAINT analysis_file_analysis_accession_fkey FOREIGN KEY (analysis_accession) REFERENCES evapro.analysis(analysis_accession);
ALTER TABLE evapro.analysis_file ADD CONSTRAINT fk_analysisfile_file_id FOREIGN KEY (file_id) REFERENCES evapro.file(file_id) MATCH FULL;



CREATE TABLE evapro.project_analysis (
	project_accession varchar(45) NOT NULL,
	analysis_accession varchar(45) NOT NULL,
	CONSTRAINT project_analysis_pkey PRIMARY KEY (project_accession, analysis_accession)
);
CREATE INDEX projanal_analacc_idx ON evapro.project_analysis USING btree (analysis_accession);
CREATE UNIQUE INDEX projanal_analproj_idx ON evapro.project_analysis USING btree (project_accession, analysis_accession);
CREATE INDEX projanal_projacc_idx ON evapro.project_analysis USING btree (project_accession);
CREATE INDEX project_analysis_analysis_accession_idx ON evapro.project_analysis USING btree (analysis_accession);
CREATE INDEX project_analysis_project_accession_idx ON evapro.project_analysis USING btree (project_accession);
COMMENT ON TABLE evapro.project_analysis IS 'Table assigning an ENA Analysis object to a project';

ALTER TABLE evapro.project_analysis OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.project_analysis TO metadata_user;

ALTER TABLE evapro.project_analysis ADD CONSTRAINT project_analysis_analysis_accession_fkey FOREIGN KEY (analysis_accession) REFERENCES evapro.analysis(analysis_accession);




CREATE SEQUENCE evapro.project_accession_code_seq
	INCREMENT BY 1
	NO MINVALUE
	MAXVALUE 9223372036854775807
	CACHE 1
	NO CYCLE;

ALTER SEQUENCE evapro.project_accession_code_seq OWNER TO metadata_user;
GRANT ALL ON SEQUENCE evapro.project_accession_code_seq TO metadata_user;

CREATE TABLE evapro.project (
	project_accession varchar(45) NOT NULL,
	center_name varchar(250) NOT NULL,
	alias varchar(4000) NULL,
	title text NULL DEFAULT NULL::character varying,
	description varchar(16000) NULL DEFAULT NULL::character varying,
	"scope" varchar(45) NOT NULL,
	material varchar(45) NOT NULL,
	selection varchar(45) NULL DEFAULT 'other'::character varying,
	"type" varchar(45) NOT NULL DEFAULT 'Umbrella'::character varying,
	secondary_study_id varchar(45) NULL,
	hold_date date NULL,
	source_type varchar(10) NOT NULL DEFAULT 'Germline'::character varying,
	project_accession_code int8 NULL DEFAULT nextval('evapro.project_accession_code_seq'::regclass),
	eva_description varchar(4000) NULL,
	eva_center_name varchar(4000) NULL,
	eva_submitter_link varchar(4000) NULL,
	eva_study_accession int8 NULL,
	ena_status int4 NULL DEFAULT 4,
	eva_status int4 NULL DEFAULT 1,
	ena_timestamp timestamp NULL,
	eva_timestamp timestamptz NULL,
	study_type varchar(100) NULL,
	CONSTRAINT pac_unq UNIQUE (project_accession_code),
	CONSTRAINT project_pkey PRIMARY KEY (project_accession)
);

CREATE OR REPLACE FUNCTION evapro.eva_study_accession_trigfunc()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
begin
if new.project_accession_code is null then
raise exception 'PROJECT_ACCESSION_CODE is NULL';
end if;
update evapro.project set eva_study_accession = project_accession_code + 1;
--delete from egapro_stage.updated_analysis_xml_txt where analysis_id=new.analysis_id;
return null;
exception
when others then raise notice 'Unable to insert eva_study_accession into project';
return null;
end;
$function$
;

ALTER FUNCTION evapro.eva_study_accession_trigfunc() OWNER TO metadata_user;
GRANT ALL ON FUNCTION evapro.eva_study_accession_trigfunc() TO metadata_user;

create trigger eva_study_accession after
insert
    or
update
    on
    evapro.project for each row execute procedure evapro.eva_study_accession_trigfunc();

ALTER TABLE evapro.project OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.project TO metadata_user;



---------------------------------------------------- Data Inserts ------------------------------------------------------

---- accessioning
INSERT INTO evapro.file_class_cv (file_class_id, file_class) VALUES(1, 'submitted');
INSERT INTO evapro.file_class_cv (file_class_id, file_class) VALUES(2, 'eva_brokered');
INSERT INTO evapro.file_class_cv (file_class_id, file_class) VALUES(3, 'eva_value_added');
INSERT INTO evapro.file_class_cv (file_class_id, file_class) VALUES(4, 'fixed_for_eva');

INSERT INTO evapro.taxonomy (taxonomy_id, common_name, scientific_name, taxonomy_code, eva_name)
VALUES(8962, 'golden eagle', 'Aquila chrysaetos', 'achrysaetos', 'golden eagle');

INSERT INTO evapro.assembly_set (assembly_set_id, taxonomy_id, assembly_name, assembly_code)
VALUES(1, 8962, 'Aquila_chrysaetos-1.0.2', 'aquilachrysaetos102');

-- should probably be filled by method load_from_ena() - (need to check)
INSERT INTO evapro.accessioned_assembly (assembly_set_id, assembly_accession, assembly_chain, assembly_version)
VALUES(1, 'GCA_000766835.1', 'GCA_000766835', 1);


INSERT INTO evapro.analysis (analysis_accession, title, alias, description, center_name, "date", vcf_reference,
                             vcf_reference_accession, hidden_in_eva, assembly_set_id)
VALUES('ERZ16299910', 'A 37K SNP array for the management and conservation of Golden Eagles (Aquila chrysaetos)',
       'Golden Eagles', 'Golden eagle SNPs utilized from a 37K Affymetrix Axiom myDesign single nucleotide polymorphism (SNP)  array.',
       'Oklahoma State University', '2023-03-09 00:00:00.000', NULL, 'GCA_000766835.1', 0, 1);

INSERT INTO evapro.project_analysis (project_accession, analysis_accession) VALUES('PRJEB60512', 'ERZ16299910');

INSERT INTO evapro.file (file_id, ena_submission_file_id, filename, file_md5, file_location, file_type, file_class,
                         file_version, is_current, ftp_file, mongo_load_status, eva_submission_file_id)
VALUES(120631, 'ERF147056879', 'goldeneagle_EVAv3.vcf.gz', 'b672aa33dc7694a052752d8ba588b1a7',
       '/usr/local/test_eva_submission/submissions/ELOAD_1/20_scratch', 'VCF', 'submitted', 1, 1,
       '/ftp.ebi.ac.uk/pub/databases/eva/PRJEB60512/goldeneagle_EVAv3.vcf.gz', 0, 'EVAF00120631');

INSERT INTO evapro.analysis_file (analysis_accession, file_id) VALUES('ERZ16299910', 120631);


---- variant load
INSERT INTO evapro.project (project_accession, center_name, alias, title, description, "scope", material, selection,
                            "type", secondary_study_id, hold_date, source_type, project_accession_code, eva_description,
                            eva_center_name, eva_submitter_link, eva_study_accession, ena_status, eva_status, ena_timestamp,
                            eva_timestamp, study_type)
VALUES('PRJEB60512', 'Oklahoma State University', 'Golden Eagles',
       'A 37K SNP array for the management and conservation of Golden Eagles (Aquila chrysaetos)',
       'Golden eagle SNPs utilized from a 37K Affymetrix Axiom myDesign single nucleotide polymorphism (SNP)  array.',
       'multi-isolate', 'DNA', 'other', 'Other', 'ERP145579', NULL, 'Germline', 1745, NULL, NULL, NULL, 1746, 4, 1,
       NULL, NULL, 'Control Set');




-- Permission on Schema
GRANT ALL ON SCHEMA evapro TO metadata_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA evapro TO metadata_user;

ALTER DATABASE metadata SET search_path TO evapro, public, "$user";
------------------------------------------       -----------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

