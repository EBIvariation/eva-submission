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



------------------------------------------------------eva_pro-----------------------------------------------------------
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
	ena_submission_file_id varchar(45) NULL DEFAULT NULL::character varying,
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
ALTER TABLE evapro.project_analysis ADD CONSTRAINT project_analysis_project_accession_fkey FOREIGN KEY (project_accession) REFERENCES evapro.project(project_accession);


CREATE TABLE evapro.eva_referenced_sequence (
	sequence_id serial4 NOT NULL,
	sequence_accession varchar(45) NOT NULL,
	"label" varchar(45) DEFAULT NULL::character varying NULL,
	ref_name varchar(45) DEFAULT NULL::character varying NULL,
	CONSTRAINT eva_referenced_sequence_pkey PRIMARY KEY (sequence_id)
);

ALTER TABLE evapro.eva_referenced_sequence OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.eva_referenced_sequence TO metadata_user;

CREATE TABLE evapro.analysis_sequence (
	analysis_accession varchar(45) NOT NULL,
	sequence_id int4 NOT NULL,
	CONSTRAINT analysis_sequence_pkey PRIMARY KEY (analysis_accession, sequence_id)
);
CREATE INDEX analysis_sequence_analysis_accession_idx ON evapro.analysis_sequence USING btree (analysis_accession);
CREATE INDEX analysis_sequence_sequence_id_idx ON evapro.analysis_sequence USING btree (sequence_id);

ALTER TABLE evapro.analysis_sequence OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.analysis_sequence TO metadata_user;


-- evapro.analysis_sequence foreign keys

ALTER TABLE evapro.analysis_sequence ADD CONSTRAINT analysis_sequence_analysis_accession_fkey FOREIGN KEY (analysis_accession) REFERENCES evapro.analysis(analysis_accession);
ALTER TABLE evapro.analysis_sequence ADD CONSTRAINT fk_analysissequence_seuqnece_id FOREIGN KEY (sequence_id) REFERENCES evapro.eva_referenced_sequence(sequence_id) MATCH FULL;


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

-- create trigger eva_study_accession after
-- insert
--     or
-- update
--     on
--     evapro.project for each row execute procedure evapro.eva_study_accession_trigfunc();

ALTER TABLE evapro.project OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.project TO metadata_user;


CREATE TABLE evapro.project_counts (
	project_accession varchar(15) NOT NULL,
	etl_count int8 NULL,
	estimate_count int8 NULL
);

ALTER TABLE evapro.project_counts OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.project_counts TO metadata_user;


CREATE TABLE evapro.eva_submission_status_cv (
	eva_submission_status_id int4 NOT NULL,
	submission_status varchar(500) NOT NULL,
	description varchar(1000) NULL,
	CONSTRAINT eva_submission_status_cv_pkey PRIMARY KEY (eva_submission_status_id)
);

ALTER TABLE evapro.eva_submission_status_cv OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.eva_submission_status_cv TO metadata_user;


CREATE TABLE evapro.submission (
	submission_id serial4 NOT NULL,
	submission_accession varchar(45) NOT NULL,
	"type" varchar(45) NOT NULL,
	"action" varchar(45) NOT NULL,
	title varchar(1000) DEFAULT NULL::character varying NULL,
	notes varchar(1000) DEFAULT NULL::character varying NULL,
	"date" timestamp DEFAULT 'now'::text::date NOT NULL,
	brokered int2 DEFAULT 0::smallint NOT NULL,
	CONSTRAINT submission_pkey PRIMARY KEY (submission_id),
	CONSTRAINT submission_submission_accession_key UNIQUE (submission_accession)
);

ALTER TABLE evapro.submission OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.submission TO metadata_user;


CREATE TABLE evapro.analysis_submission (
	analysis_accession varchar(45) NOT NULL,
	submission_id int4 NOT NULL,
	CONSTRAINT analysis_submission_pkey PRIMARY KEY (analysis_accession, submission_id),
	CONSTRAINT analysis_submission_analysis_accession_fkey FOREIGN KEY (analysis_accession) REFERENCES evapro.analysis(analysis_accession),
	CONSTRAINT fk_analysissubmission_submission_id FOREIGN KEY (submission_id) REFERENCES evapro.submission(submission_id) MATCH FULL
);

CREATE INDEX analysis_submission_analysis_accession_idx ON evapro.analysis_submission USING btree (analysis_accession);
CREATE INDEX analysis_submission_submission_id_idx ON evapro.analysis_submission USING btree (submission_id);

ALTER TABLE evapro.analysis_submission OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.analysis_submission TO metadata_user;


CREATE TABLE evapro.eva_submission (
	eva_submission_id serial4 NOT NULL,
	eva_submission_status_id int4 NOT NULL,
	hold_date date NULL,
	CONSTRAINT eva_submission_pkey PRIMARY KEY (eva_submission_id)
);

ALTER TABLE evapro.eva_submission OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.eva_submission TO metadata_user;

ALTER TABLE evapro.eva_submission ADD CONSTRAINT fk_evasubmission_eva_submission_status_id FOREIGN KEY (eva_submission_status_id) REFERENCES evapro.eva_submission_status_cv(eva_submission_status_id) MATCH FULL;


CREATE TABLE evapro.project_eva_submission (
	project_accession varchar(25) NOT NULL,
	old_ticket_id int4 NOT NULL,
	eload_id int4 NULL,
	old_eva_submission_id int4 NULL,
	CONSTRAINT project_eva_submission_pkey PRIMARY KEY (project_accession, old_ticket_id)
);

ALTER TABLE evapro.project_eva_submission OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.project_eva_submission TO metadata_user;

CREATE TABLE evapro.project_ena_submission (
	project_accession varchar(45) NOT NULL,
	submission_id int4 NOT NULL,
	CONSTRAINT project_submission_pkey PRIMARY KEY (project_accession,submission_id)
);
CREATE INDEX project_submission_project_accession_idx ON evapro.project_ena_submission (project_accession);
CREATE INDEX project_submission_submission_id_idx ON evapro.project_ena_submission (submission_id);

ALTER TABLE evapro.project_ena_submission OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.project_ena_submission TO metadata_user;

CREATE TABLE evapro.project_taxonomy (
	project_accession varchar(45) NOT NULL,
	taxonomy_id int4 NOT NULL,
	CONSTRAINT project_taxonomy_pkey PRIMARY KEY (project_accession, taxonomy_id)
);

CREATE INDEX project_taxonomy_project_accession_idx ON evapro.project_taxonomy USING btree (project_accession);
CREATE INDEX project_taxonomy_taxonomy_id_idx ON evapro.project_taxonomy USING btree (taxonomy_id);

ALTER TABLE evapro.project_taxonomy OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.project_taxonomy TO metadata_user;

ALTER TABLE evapro.project_taxonomy ADD CONSTRAINT fk_project_taxonomy_taxonomy_id FOREIGN KEY (taxonomy_id) REFERENCES evapro.taxonomy(taxonomy_id) MATCH FULL;


CREATE TABLE evapro.linked_project (
	linked_project_id serial4 NOT NULL,
	project_accession varchar(45) NOT NULL,
	linked_project_accession varchar(45) NOT NULL,
	linked_project_relation varchar(45) NOT NULL,
	link_live_for_eva bool NULL DEFAULT false,
	CONSTRAINT linked_project_pkey PRIMARY KEY (linked_project_id)
);
CREATE INDEX linked_project_project_accession_idx ON evapro.linked_project USING btree (project_accession);

ALTER TABLE evapro.linked_project OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.linked_project TO metadata_user;


CREATE OR REPLACE VIEW evapro.project_children_taxonomy
AS SELECT y.project_accession,
    string_agg(DISTINCT y.child_project::text, ', '::text) AS child_projects,
    string_agg(DISTINCT y.tax_id::text, ', '::text) AS taxonomy_ids,
    string_agg(DISTINCT y.common_name, ', '::text) AS taxonomy_common_names,
    string_agg(DISTINCT y.sci_name::text, ', '::text) AS taxonomy_scientific_names
   FROM ( SELECT x.eva_ticket,
            x.project_accession,
            x.child_project,
            x.tax_id,
            upper(substr(COALESCE(evapro.taxonomy.eva_name, evapro.taxonomy.common_name)::text, 1, 1)) || substr(COALESCE(evapro.taxonomy.eva_name, evapro.taxonomy.common_name)::text, 2, length(COALESCE(evapro.taxonomy.eva_name, evapro.taxonomy.common_name)::text) - 1) AS common_name,
            evapro.taxonomy.scientific_name AS sci_name
           FROM ( SELECT z.eva_ticket,
                    z.project_accession,
                    COALESCE(z.child_2, z.child1) AS child_project,
                    COALESCE(z.taxonomy_id, COALESCE(z.tax_id_child1, z.tax_id_child2)) AS tax_id
                   FROM ( SELECT 'EVA-'::text || evapro.eva_submission.eva_submission_id AS eva_ticket,
                            ps.project_accession,
                            pt.taxonomy_id,
                            cp.child AS child1,
                            cp.child_tax_id AS tax_id_child1,
                            cp1.child AS child_2,
                            cp1.child_tax_id AS tax_id_child2
                           FROM evapro.eva_submission
                             LEFT JOIN evapro.project_eva_submission ps(project_accession, eva_submission_id, eload_id, old_eva_submission_id) USING (eva_submission_id)
                             LEFT JOIN evapro.project_taxonomy pt USING (project_accession)
                             LEFT JOIN ( SELECT p.project_accession,
                                    p.type,
                                    lp.project_accession AS child,
                                    pt_1.taxonomy_id AS child_tax_id
                                   FROM evapro.project p
                                     JOIN evapro.linked_project lp ON lp.linked_project_accession::text = p.project_accession::text
                                     LEFT JOIN evapro.project_taxonomy pt_1 ON lp.project_accession::text = pt_1.project_accession::text
                                  WHERE p.type::text = 'Umbrella'::text) cp(project_accession_1, type, child, child_tax_id) ON ps.project_accession::text = cp.project_accession_1::text
                             LEFT JOIN ( SELECT p.project_accession,
                                    p.type,
                                    lp.project_accession AS child,
                                    pt_1.taxonomy_id AS child_tax_id
                                   FROM evapro.project p
                                     JOIN evapro.linked_project lp ON lp.linked_project_accession::text = p.project_accession::text
                                     LEFT JOIN evapro.project_taxonomy pt_1 ON lp.project_accession::text = pt_1.project_accession::text
                                  WHERE p.type::text = 'Umbrella'::text AND NOT (lp.project_accession::text IN ( SELECT linked_project.linked_project_accession
   FROM evapro.linked_project))) cp1(project_accession_1, type, child, child_tax_id) ON cp.child::text = cp1.project_accession_1::text) z) x
             JOIN evapro.taxonomy ON x.tax_id = evapro.taxonomy.taxonomy_id) y
  WHERE y.project_accession IS NOT NULL
  GROUP BY y.project_accession;

ALTER TABLE evapro.project_children_taxonomy OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.project_children_taxonomy TO metadata_user;


CREATE SEQUENCE evapro.pro_samp1_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 43
	CACHE 1
	NO CYCLE;

ALTER SEQUENCE evapro.pro_samp1_seq OWNER TO metadata_user;
GRANT ALL ON SEQUENCE evapro.pro_samp1_seq TO metadata_user;


CREATE TABLE evapro.sample (
	sample_id serial4 NOT NULL,
	biosample_accession varchar(45) NOT NULL,
	ena_accession varchar(45) NOT NULL,
	CONSTRAINT sample_pkey PRIMARY KEY (sample_id)
);

ALTER TABLE evapro.sample OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.sample TO metadata_user;

CREATE TABLE evapro.file_sample (
	file_id int4 NOT NULL,
    sample_id int4 NOT NULL,
	name_in_file varchar(250) NOT NULL,
	CONSTRAINT file_sample_pkey PRIMARY KEY (file_id, sample_id)
);

CREATE INDEX filesamp_fileid_idx ON evapro.file_sample USING btree (file_id);
CREATE UNIQUE INDEX filesamp_filesamp_idx ON evapro.file_sample USING btree (file_id, sample_id);
CREATE INDEX filesamp_sampleid_idx ON evapro.file_sample USING btree (sample_id);
COMMENT ON TABLE evapro.file_sample IS 'Links sample to a file';

ALTER TABLE evapro.file_sample OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.file_sample TO metadata_user;

CREATE TABLE evapro.project_samples_temp1 (
	project_accession varchar(15) NOT NULL,
	sample_count int4 NULL,
	pro_samp1_id int4 NOT NULL DEFAULT nextval('evapro.pro_samp1_seq'::regclass),
	CONSTRAINT project_samples_temp1_pkey PRIMARY KEY (project_accession)
);

ALTER TABLE evapro.project_samples_temp1 ADD CONSTRAINT project_samples_temp1_project_accession_fkey FOREIGN KEY (project_accession) REFERENCES evapro.project(project_accession);
ALTER TABLE evapro.project_samples_temp1 OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.project_samples_temp1 TO metadata_user;

CREATE TABLE evapro.experiment_type (
	experiment_type_id serial4 NOT NULL,
	experiment_type varchar(45) NOT NULL,
	CONSTRAINT experiment_type_pkey PRIMARY KEY (experiment_type_id)
);

ALTER TABLE evapro.experiment_type OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.experiment_type TO metadata_user;

CREATE TABLE evapro.analysis_experiment_type (
	analysis_accession varchar(45) NOT NULL,
	experiment_type_id int4 NOT NULL,
	CONSTRAINT analysis_experiment_type_pkey PRIMARY KEY (analysis_accession, experiment_type_id)
);
CREATE INDEX analysis_experiment_type_analysis_accession_idx ON evapro.analysis_experiment_type USING btree (analysis_accession);
CREATE INDEX analysis_experiment_type_experiment_type_id_idx ON evapro.analysis_experiment_type USING btree (experiment_type_id);

ALTER TABLE evapro.analysis_experiment_type OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.analysis_experiment_type TO metadata_user;

ALTER TABLE evapro.analysis_experiment_type ADD CONSTRAINT analysis_experiment_type_analysis_accession_fkey FOREIGN KEY (analysis_accession) REFERENCES evapro.analysis(analysis_accession);
ALTER TABLE evapro.analysis_experiment_type ADD CONSTRAINT fk_analysisexperimenttype_experiment_type_id FOREIGN KEY (experiment_type_id) REFERENCES evapro.experiment_type(experiment_type_id) MATCH FULL;


CREATE TABLE evapro.experiment_cv (
	experiment_type varchar(250) NOT NULL,
	CONSTRAINT experiment_cv_pkey PRIMARY KEY (experiment_type)
);
COMMENT ON TABLE evapro.experiment_cv IS 'The Experiment Type CV from the Analysis XSD';

ALTER TABLE evapro.experiment_cv OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.experiment_cv TO metadata_user;


CREATE TABLE evapro.display_experiment_type (
	experiment_type varchar(45) NOT NULL,
	display_type varchar(45) NOT NULL
);

ALTER TABLE evapro.display_experiment_type OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.display_experiment_type TO metadata_user;

ALTER TABLE evapro.display_experiment_type ADD CONSTRAINT display_experiment_type_experiment_type_fkey FOREIGN KEY (experiment_type) REFERENCES evapro.experiment_cv(experiment_type);


CREATE OR REPLACE VIEW evapro.project_experiment
AS SELECT de.project_accession,
    string_agg(de.experiment_type::text, ', '::text) AS experiment_type,
    string_agg(de.display_type::text, ', '::text) AS display_type,
    string_agg(de.display_type::text, ', '::text) AS experiment_type_abbreviation
   FROM ( SELECT DISTINCT pe1.project_accession,
            pe1.experiment_type,
            pe1.display_type
           FROM ( SELECT DISTINCT evapro.project_analysis.project_accession,
                    evapro.display_experiment_type.experiment_type,
                    evapro.display_experiment_type.display_type
                   FROM evapro.project_analysis
                     JOIN evapro.analysis USING (analysis_accession)
                     JOIN evapro.analysis_experiment_type USING (analysis_accession)
                     JOIN evapro.experiment_type USING (experiment_type_id)
                     JOIN evapro.display_experiment_type USING (experiment_type)
                  WHERE analysis.hidden_in_eva <> 1
                UNION
                 SELECT DISTINCT evapro.linked_project.linked_project_accession,
                    evapro.display_experiment_type.experiment_type,
                    evapro.display_experiment_type.display_type
                   FROM evapro.linked_project
                     JOIN evapro.project_analysis USING (project_accession)
                     JOIN evapro.analysis USING (analysis_accession)
                     JOIN evapro.analysis_experiment_type USING (analysis_accession)
                     JOIN evapro.experiment_type USING (experiment_type_id)
                     JOIN evapro.display_experiment_type USING (experiment_type)
                  WHERE evapro.analysis.hidden_in_eva <> 1 AND linked_project.link_live_for_eva IS TRUE) pe1
          ORDER BY pe1.display_type DESC) de
  GROUP BY de.project_accession;

ALTER TABLE evapro.project_experiment OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.project_experiment TO metadata_user;


CREATE TABLE evapro.dbxref (
	dbxref_id serial4 NOT NULL,
	db varchar(45) NOT NULL,
	id varchar(45) NOT NULL,
	"label" varchar(250) NULL DEFAULT NULL::character varying,
	link_type varchar(100) NOT NULL,
	source_object varchar(100) NOT NULL,
	CONSTRAINT dbxref_db_id_key UNIQUE (db, id),
	CONSTRAINT dbxref_pkey PRIMARY KEY (dbxref_id)
);

ALTER TABLE evapro.dbxref OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.dbxref TO metadata_user;


CREATE TABLE evapro.project_dbxref (
	project_accession varchar(45) NOT NULL,
	dbxref_id int4 NOT NULL,
	CONSTRAINT project_dbxref_pkey PRIMARY KEY (project_accession, dbxref_id)
);
CREATE INDEX project_dbxref_dbxref_id_idx ON evapro.project_dbxref USING btree (dbxref_id);
CREATE INDEX project_dbxref_project_accession_idx ON evapro.project_dbxref USING btree (project_accession);

ALTER TABLE evapro.project_dbxref OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.project_dbxref TO metadata_user;

ALTER TABLE evapro.project_dbxref ADD CONSTRAINT project_dbxref_dbxref_id_fkey FOREIGN KEY (dbxref_id) REFERENCES evapro.dbxref(dbxref_id);


CREATE OR REPLACE VIEW evapro.project_publication
AS SELECT evapro.project_dbxref.project_accession,
    evapro.dbxref.db,
    evapro.dbxref.id
   FROM evapro.project_dbxref
     JOIN evapro.dbxref USING (dbxref_id)
  WHERE evapro.dbxref.db::text ILIKE 'PubMed'::text OR evapro.dbxref.db::text ILIKE 'doi'::text;

ALTER TABLE evapro.project_publication OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.project_publication TO metadata_user;


CREATE OR REPLACE VIEW evapro.project_reference
AS SELECT DISTINCT a.project_accession,
    a.reference_accession,
    a.reference_name
   FROM ( SELECT pa.project_accession,
            evapro.analysis.vcf_reference_accession AS reference_accession,
            evapro.analysis.vcf_reference AS reference_name
           FROM evapro.linked_project lp
             JOIN evapro.project_analysis pa ON lp.linked_project_accession::text = pa.project_accession::text
             JOIN evapro.analysis USING (analysis_accession)
          WHERE evapro.analysis.hidden_in_eva <> 1
        UNION
         SELECT lp.linked_project_accession,
            evapro.analysis.vcf_reference_accession AS reference_accession,
            evapro.analysis.vcf_reference AS reference_name
           FROM evapro.linked_project lp
             JOIN evapro.project_analysis pa ON lp.project_accession::text = pa.project_accession::text
             JOIN evapro.analysis USING (analysis_accession)
          WHERE evapro.analysis.hidden_in_eva <> 1
        UNION
         SELECT pa.project_accession,
            evapro.analysis.vcf_reference_accession AS reference_accession,
            evapro.analysis.vcf_reference AS reference_name
           FROM evapro.analysis
             JOIN evapro.project_analysis pa USING (analysis_accession)
          WHERE evapro.analysis.hidden_in_eva <> 1) a;

ALTER TABLE evapro.project_reference OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.project_reference TO metadata_user;


CREATE TABLE evapro.platform (
	platform_id serial4 NOT NULL,
	platform varchar(4000) NOT NULL,
	manufacturer varchar(100) NULL,
	CONSTRAINT platform_pkey PRIMARY KEY (platform_id)
);

ALTER TABLE evapro.platform OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.platform TO metadata_user;


CREATE TABLE evapro.analysis_platform (
	analysis_accession varchar(45) NOT NULL,
	platform_id int4 NOT NULL,
	CONSTRAINT analysis_platform_pkey PRIMARY KEY (analysis_accession, platform_id)
);
CREATE INDEX analysis_platform_analysis_accession_idx ON evapro.analysis_platform USING btree (analysis_accession);
CREATE INDEX analysis_platform_platform_id_idx ON evapro.analysis_platform USING btree (platform_id);

ALTER TABLE evapro.analysis_platform OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.analysis_platform TO metadata_user;

ALTER TABLE evapro.analysis_platform ADD CONSTRAINT analysis_platform_analysis_accession_fkey FOREIGN KEY (analysis_accession) REFERENCES evapro.analysis(analysis_accession);
ALTER TABLE evapro.analysis_platform ADD CONSTRAINT fk_analysisplatform_platform_id FOREIGN KEY (platform_id) REFERENCES evapro.platform(platform_id) MATCH FULL;


CREATE OR REPLACE VIEW evapro.project_platform
AS SELECT DISTINCT pp.project_accession,
    pp.platform
   FROM ( SELECT DISTINCT evapro.project_analysis.project_accession,
            evapro.platform.platform
           FROM evapro.analysis
             JOIN evapro.project_analysis USING (analysis_accession)
             JOIN evapro.analysis_platform USING (analysis_accession)
             JOIN evapro.platform USING (platform_id)
             JOIN evapro.project USING (project_accession)
          WHERE analysis.hidden_in_eva <> 1
        UNION
         SELECT DISTINCT evapro.linked_project.linked_project_accession,
            evapro.platform.platform
           FROM evapro.analysis
             JOIN evapro.project_analysis USING (analysis_accession)
             JOIN evapro.linked_project USING (project_accession)
             JOIN evapro.analysis_platform USING (analysis_accession)
             JOIN evapro.platform USING (platform_id)
             JOIN evapro.project USING (project_accession)
          WHERE analysis.hidden_in_eva <> 1 AND linked_project.link_live_for_eva IS TRUE) pp;

ALTER TABLE evapro.project_platform OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.project_platform TO metadata_user;


CREATE TABLE evapro.project_resource (
	project_accession varchar(45) NOT NULL,
	resource varchar(250) NOT NULL
);

ALTER TABLE evapro.project_resource OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.project_resource TO metadata_user;


CREATE MATERIALIZED VIEW evapro.study_browser
AS SELECT DISTINCT evapro.project.project_accession,
    evapro.project.eva_study_accession AS study_id,
    evapro.project.title AS project_title,
    COALESCE(evapro.project.eva_description, evapro.project.description) AS description,
    COALESCE(evapro.project_children_taxonomy.taxonomy_ids, '-'::text) AS tax_id,
    COALESCE(evapro.project_children_taxonomy.taxonomy_common_names, '-'::text) AS common_name,
    COALESCE(evapro.project_children_taxonomy.taxonomy_scientific_names, '-'::text) AS scientific_name,
    COALESCE(evapro.project.source_type, '-'::character varying) AS source_type,
    COALESCE(evapro.project.study_type, '-'::character varying) AS study_type,
    COALESCE(evapro.project_counts.etl_count, evapro.project_counts.estimate_count) AS variant_count,
    evapro.project_samples_temp1.sample_count AS samples,
    COALESCE(evapro.project.eva_center_name, evapro.project.center_name) AS center,
    COALESCE(evapro.project.scope, '-'::character varying) AS scope,
    COALESCE(evapro.project.material, '-'::character varying) AS material,
    COALESCE(c.ids, '-'::text) AS publications,
    COALESCE(evapro.project_children_taxonomy.child_projects, '-'::text) AS associated_projects,
    COALESCE(initcap(evapro.project_experiment.experiment_type), '-'::text) AS experiment_type,
    COALESCE(evapro.project_experiment.experiment_type_abbreviation, '-'::text) AS experiment_type_abbreviation,
    COALESCE(a.v_ref, '-'::text) AS assembly_accession,
    COALESCE(b.v_ref, '-'::text) AS assembly_name,
    COALESCE(d.platform, '-'::text) AS platform,
    COALESCE(r.resource, '-'::text::character varying) AS resource,
    COALESCE(browsable_table.browsable, false) AS browsable
   FROM evapro.project
     LEFT JOIN evapro.project_counts USING (project_accession)
     LEFT JOIN evapro.project_children_taxonomy USING (project_accession)
     LEFT JOIN evapro.project_samples_temp1 USING (project_accession)
     LEFT JOIN evapro.project_eva_submission project_eva_submission(project_accession, eva_submission_id, eload_id, old_eva_submission_id) USING (project_accession)
     LEFT JOIN evapro.project_experiment USING (project_accession)
     LEFT JOIN ( SELECT evapro.project_publication.project_accession,
            string_agg(evapro.project_publication.db::text||':'||evapro.project_publication.id::text, ', '::text) AS ids
           FROM evapro.project_publication
          GROUP BY evapro.project_publication.project_accession) c(project_accession_1, ids) ON c.project_accession_1::text = project.project_accession::text
     LEFT JOIN ( SELECT evapro.project_reference.project_accession,
            string_agg(evapro.project_reference.reference_accession::text, ', '::text) AS v_ref
           FROM evapro.project_reference
          GROUP BY evapro.project_reference.project_accession) a(project_accession_1, v_ref) ON a.project_accession_1::text = project.project_accession::text
     LEFT JOIN ( SELECT evapro.project_reference.project_accession,
            string_agg(evapro.project_reference.reference_name::text, ', '::text) AS v_ref
           FROM evapro.project_reference
          GROUP BY evapro.project_reference.project_accession) b(project_accession_1, v_ref) ON b.project_accession_1::text = project.project_accession::text
     LEFT JOIN ( SELECT evapro.project_platform.project_accession,
            string_agg(evapro.project_platform.platform::text, ', '::text) AS platform
           FROM evapro.project_platform
          GROUP BY evapro.project_platform.project_accession) d(project_accession_1, platform) ON d.project_accession_1::text = project.project_accession::text
     JOIN evapro.eva_submission USING (eva_submission_id)
     LEFT JOIN evapro.project_resource r USING (project_accession)
     LEFT JOIN ( SELECT evapro.browsable_file.project_accession,
            bool_or(evapro.browsable_file.loaded) AS browsable
           FROM evapro.browsable_file
          GROUP BY evapro.browsable_file.project_accession) browsable_table(project_accession_browsable, browsable) ON browsable_table.project_accession_browsable::text = project.project_accession::text
  WHERE (project.hold_date <= now()::date OR project.hold_date IS NULL) AND eva_submission.eva_submission_status_id >= 6 AND project.ena_status = 4 AND project.eva_status = 1
WITH DATA;


ALTER TABLE evapro.study_browser OWNER TO metadata_user;
GRANT ALL ON TABLE evapro.study_browser TO metadata_user;





---------------------------------------------------- Data Inserts ------------------------------------------------------

INSERT INTO evapro.file_class_cv (file_class_id, file_class) VALUES(1, 'submitted');
INSERT INTO evapro.file_class_cv (file_class_id, file_class) VALUES(2, 'eva_brokered');
INSERT INTO evapro.file_class_cv (file_class_id, file_class) VALUES(3, 'eva_value_added');
INSERT INTO evapro.file_class_cv (file_class_id, file_class) VALUES(4, 'fixed_for_eva');

-------------

INSERT INTO evapro.taxonomy (taxonomy_id, common_name, scientific_name, taxonomy_code, eva_name)
VALUES(4006, 'flax', 'Linum usitatissimum', 'lusitatissimum', 'flax');

INSERT INTO evapro.assembly_set (taxonomy_id, assembly_name, assembly_code)
VALUES(4006, 'ASM22429v2', 'asm22429v2');

INSERT INTO evapro.accessioned_assembly (assembly_set_id, assembly_accession, assembly_chain, assembly_version)
VALUES(1, 'GCA_000224295.2', 'GCA_000224295', 2);

INSERT INTO evapro.file (ena_submission_file_id, filename, file_md5, file_location, file_type, file_class, file_version, is_current, ftp_file, mongo_load_status, eva_submission_file_id)
VALUES('ERF153535013', 'Flax_SNP_variants.vcf.gz', 'd9918c2d697700f732a117576fc97ff7', '/nfs/production/keane/eva/submissions/ELOAD_1145/20_scratch', 'VCF', 'submitted', 1, 1, '/ftp.ebi.ac.uk/pub/databases/eva/PRJEB62432/Flax_SNP_variants.vcf.gz', 0, 'EVAF00120867');

INSERT INTO evapro.browsable_file (file_id, ena_submission_file_id, filename, loaded, eva_release, deleted, eva_release_deleted, project_accession, loaded_assembly, assembly_set_id)
VALUES(1, 'ERF153535013', 'Flax_SNP_variants.vcf.gz', true, '20230521', false, 'None', 'PRJEB62432', 'GCA_000224295.2', 1);

INSERT INTO evapro.supported_assembly_tracker (taxonomy_id, "source", assembly_id, "current", start_date)
VALUES(4006, 'Ensembl', 'GCA_000224295.2', true, '2021-01-01');

INSERT INTO evapro.project (project_accession, center_name, alias, title, description, "scope", material, selection, "type", secondary_study_id, hold_date, source_type, eva_description, eva_center_name, eva_submitter_link, ena_status, eva_status, ena_timestamp, eva_timestamp, study_type)
VALUES ('PRJEB62432', 'NDSU', 'IFQT', 'Improvement of Flax Quantitative Traits', 'The study was done to analyze the genetic diversity, identify SNPs and genes associated to specific traits and optimize genomic selection models in NDSU Flax core collection.', 'multi-isolate', 'DNA', 'other', 'Other', 'ERP147519', NULL, 'Germline', NULL, NULL, NULL, 4, 1, NULL, NULL, 'Control Set');

INSERT INTO evapro.project_taxonomy (project_accession, taxonomy_id)
VALUES ('PRJEB62432', 4006);

-- required for web services to work, "temp1" name notwithstanding
INSERT INTO evapro.project_samples_temp1 (project_accession, sample_count)
VALUES ('PRJEB62432', 100);

INSERT INTO evapro.dbxref (db, id, link_type, source_object)
VALUES ('doi', '10.3389/fgene.2023.1229741', 'publication', 'project'),
       ('PubMed', '37955008', 'publication', 'project');

INSERT INTO evapro.project_dbxref (project_accession, dbxref_id)
VALUES ('PRJEB62432', 1),
       ('PRJEB62432', 2);

-- A valid submission status is required to get the study to appear in the study browser materialized view

INSERT INTO evapro.eva_submission_status_cv (eva_submission_status_id, submission_status, description)
VALUES (0,'Submission Defined','A submission has been initiated, but not files yet received'),
       (1,'Files Received','EVA has receieved the submission files'),
       (2,'VCF File Valid','The VCF files are technically valid'),
       (3,'Meta-data Valid','The required meta-data is complete and correct'),
       (4,'ENA Project Accession Assigned','A project accession has be assigned by ENA'),
       (5,'File submitted to ENA','The VCF files have been submitted to ENA'),
       (6,'ENA Submission Complete','The ENA submission is complete'),
       (7,'EVA Processing Started','The files have started to be processed by EVA'),
       (8,'EVA Processing Complete','The files have completed processing by EVA'),
       (9,'Mongo Loading Started','The data is loading to MongoDB'),
       (10,'Submission Live','The submission is live and public at EVA'),
       (-1,'Submission Private','The submission is private at EVA (e.g. hide parent project)');

INSERT INTO evapro.eva_submission (eva_submission_status_id)
VALUES (6);

INSERT INTO evapro.project_eva_submission (project_accession, old_ticket_id, eload_id)
VALUES ('PRJEB62432', 1, 42);

INSERT INTO evapro.platform (platform_id, platform, manufacturer)
VALUES ( 1,'Illumina HiSeq 2000','Illumina'),
( 2,'Illumina HiSeq 2500','Illumina'),
( 3,'AB SOLiD System','AB'),
( 4,'AB SOLiD System 2.0','AB'),
( 5,'454 GS FLX','454'),
( 6,'Illumina Genome Analyzer II','Illumina'),
( 7,'Illumina HiSeq 1000','Illumina'),
( 8,'Illumina HiScanSQ','Illumina'),
( 9,'Illumina MiSeq','Illumina'),
(10,'Illumina Genome Analyzer','Illumina'),
(11,'Illumina Genome Analyzer IIx','Illumina'),
(12,'454 GS','454'),
(13,'454 GS 20','454'),
(14,'454 GS FLX+','454'),
(15,'454 GS FLX Titanium','454'),
(16,'454 GS Junior','454'),
(17,'AB SOLiD System 3.0','AB'),
(18,'AB SOLiD 3 Plus System','AB'),
(19,'AB SOLiD 4 System','AB'),
(20,'AB SOLiD 4hq System','AB'),
(21,'AB SOLiD PI System','AB'),
(22,'AB 5500 Genetic Analyzer','AB'),
(23,'AB 5500xl Genetic Analyzer','AB'),
(24,'Complete Genomics','Complete Genomics'),
(25,'unspecified','unspecified'),
(26,'Illumina','Illumina'),
(27,'Ion Torrent PGM','ION_TORRENT'),
(30,'Ion Torrent Proton','ION_TORRENT'),
(31,'unspecified','ION_TORRENT'),
(32,'Illumina HiSeq X Ten','Illumina'),
(33,'Ion S5XL','ION_TORRENT'),
(34,'Ion Personal Genome Machine (PGM) System v2','ION_TORRENT'),
(35,'Affymetrix','Affymetrix'),
(36,'Illumina HiSeq 4000','Illumina'),
(37,'Illumina NextSeq 500','Illumina'),
(38,'Illumina HiSeq 3500','Illumina'),
(39,'Illumina NovaSeq 6000','Illumina'),
(40,'Nimblegen 4.2M Probe Custom DNA Microarray','Roche'),
(41,'AB 3300 Genetic Analyzer','AB'),
(42,'Oxford Nanopore PromethION','Oxford Nanopore'),
(43,'ABI PRISM 310 Genetic Analyzer','ABI'),
(44,'Illumina Hiseq Xten','Illumina'),
(45,'AB 3730xl','AB'),
(46,'Illumina MiniSeq','Illumina'),
(47,'MGISEQ-2000','MGI'),
(48,'Illumina CanineHD','Illumina'),
(49,'AB 3730xl','AB'),
(50,'AB 3130 Genetic Analyzer','AB'),
(51,'Bio-Rad CFX96','Bio-Rad'),
(52,'ABI 3500 Genetic Analyzer','ABI'),
(53,'Illumina iScan','Illumina'),
(54,'BGISEQ-500','BGI'),
(55,'MassARRAY iPLEX','AgenaBioscience'),
(56,'Ion S5','ION_TORRENT');

REFRESH MATERIALIZED VIEW evapro.study_browser;

------------------- Permission on Schema
GRANT ALL ON SCHEMA evapro TO metadata_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA evapro TO metadata_user;

ALTER USER metadata_user SET SEARCH_PATH TO evapro;
ALTER DATABASE metadata SET search_path TO  evapro, public, "$user";
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

