# coding: utf-8
from sqlalchemy import BigInteger, Boolean, CHAR, Column, Date, DateTime, ForeignKey, ForeignKeyConstraint, Index, \
    Integer, JSON, Numeric, SmallInteger, String, Table, Text, UniqueConstraint, text, func
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata

t_assembly_accessioning_store_status = Table(
    'assembly_accessioning_store_status', metadata,
    Column('assembly_accession', String(25), nullable=False, unique=True),
    Column('assembly_in_accessioning_store', Boolean)
)

t_assembly_species = Table(
    'assembly_species', metadata,
    Column('assembly_accession', String(25)),
    Column('taxonomy_id', Integer)
)

# NOT USED
class CallingAlgorithm(Base):
    __tablename__ = 'calling_algorithm'
    __table_args__ = {'comment': 'Calling algorithm details'}

    calling_algorithm_id = Column(Integer, primary_key=True)
    description = Column(String(250), nullable=False)


class CountStat(Base):
    __tablename__ = 'count_stats'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
                # server_default=text("nextval('count_stats_id_seq'::regclass)"))
    count = Column(BigInteger, nullable=False)
    identifier = Column(JSON, nullable=False)
    metric = Column(String(255), nullable=False)
    process = Column(String(255), nullable=False)
    timestamp = Column(DateTime, nullable=False, default=func.now())

# Probably not useful anymore
class DbsnpStatusCv(Base):
    __tablename__ = 'dbsnp_status_cv'

    dbsnp_status_id = Column(Integer, primary_key=True)
    submission_status = Column(String(500), nullable=False)
    description = Column(String(1000))

    eva_submissions = relationship('EvaSubmission', secondary='dbsnp_submission_status')


class Dbxref(Base):
    __tablename__ = 'dbxref'
    __table_args__ = (
        UniqueConstraint('db', 'id'),
        {'comment': 'List of DBXREFs'}
    )

    dbxref_id = Column(Integer, primary_key=True, autoincrement=True)
                       # server_default=text("nextval('dbxref_dbxref_id_seq'::regclass)"))
    db = Column(String(45), nullable=False)
    id = Column(String(45), nullable=False)
    label = Column(String(250), server_default=text("NULL"))
    link_type = Column(String(100), nullable=False)
    source_object = Column(String(100), nullable=False)

    samples = relationship('Sample', secondary='sample_dbxref')


t_dgva_study_table = Table(
    'dgva_study_table', metadata,
    Column('study_accession', String(15)),
    Column('call_count', Integer),
    Column('region_count', Integer),
    Column('alias', String(100)),
    Column('tax_id', Integer),
    Column('species_scientific_name', String(100)),
    Column('pubmed_id', Integer),
    Column('display_name', String(1000)),
    Column('study_type', String(100)),
    Column('project_id', String(100)),
    Column('study_url', String(4000)),
    Column('study_description', String(4000)),
    Column('sample_count', Integer),
    Column('subject_count', Integer),
    Column('assembly_accession', String(50)),
    Column('assembly_name', String(50))
)

# NOT USED
class EvaPrefix(Base):
    __tablename__ = 'eva_prefixes'

    prefix_id = Column(SmallInteger, primary_key=True)
    prefix_type = Column(String(25), nullable=False)
    prefix = Column(String(10), nullable=False, unique=True)
    description = Column(String(250), nullable=False)


class EvaReferencedSequence(Base):
    __tablename__ = 'eva_referenced_sequence'
    __table_args__ = {'comment': 'Sequences referenced by EVA '}

    sequence_id = Column(Integer, primary_key=True, autoincrement=True)
                         # server_default=text("nextval('eva_referenced_sequence_sequence_id_seq'::regclass)"))
    sequence_accession = Column(String(45), nullable=False)
    label = Column(String(45), server_default=text("NULL"))
    ref_name = Column(String(45), server_default=text("NULL"))


class EvaSubmissionStatusCv(Base):
    __tablename__ = 'eva_submission_status_cv'

    eva_submission_status_id = Column(Integer, primary_key=True)
    submission_status = Column(String(500), nullable=False)
    description = Column(String(1000))


class ExperimentCv(Base):
    __tablename__ = 'experiment_cv'
    __table_args__ = {'comment': 'The Experiment Type CV from the Analysis XSD'}

    experiment_type = Column(String(250), primary_key=True)


class ExperimentType(Base):
    __tablename__ = 'experiment_type'
    __table_args__ = {'comment': 'Experiment type'}

    experiment_type_id = Column(Integer, primary_key=True, autoincrement=True)
                                # server_default=text("nextval('experiment_type_experiment_type_id_seq'::regclass)"))
    experiment_type = Column(String(45), nullable=False)


class FileClassCv(Base):
    __tablename__ = 'file_class_cv'
    __table_args__ = {'comment': 'List of the file classes EVA stores'}

    file_class_id = Column(Integer, primary_key=True, autoincrement=True)
                           # server_default=text("nextval('file_class_cv_file_class_id_seq'::regclass)"))
    file_class = Column(String(45), nullable=False, unique=True)


class LinkedProject(Base):
    __tablename__ = 'linked_project'
    __table_args__ = {'comment': 'Table allowing a project to link to another project'}

    linked_project_id = Column(Integer, primary_key=True, autoincrement=True)
                               # server_default=text("nextval('linked_project_linked_project_id_seq'::regclass)"))
    project_accession = Column(String(45), nullable=False, index=True)
    linked_project_accession = Column(String(45), nullable=False)
    linked_project_relation = Column(String(45), nullable=False)

    link_live_for_eva = Column(Boolean, server_default=text("false"))


class Platform(Base):
    __tablename__ = 'platform'
    __table_args__ = {'comment': 'Platform referenced by EVA'}

    platform_id = Column(Integer, primary_key=True, autoincrement=True)
                         # server_default=text("nextval('platform_platform_id_seq'::regclass)"))
    platform = Column(String(4000), nullable=False)
    manufacturer = Column(String(100))

# NOT USED
class ProcessCountMetric(Base):
    __tablename__ = 'process_count_metric'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
                # server_default=text("nextval('process_count_metric_id_seq'::regclass)"))
    count = Column(BigInteger, nullable=False)
    identifier = Column(JSON, nullable=False)
    metric = Column(String(255), nullable=False)
    process = Column(String(255), nullable=False)


class Project(Base):
    __tablename__ = 'project'
    __table_args__ = {'comment': 'Table storing the project details (project as assigned by ENA'}

    project_accession = Column(String(45), primary_key=True)
    center_name = Column(String(250), nullable=False)
    alias = Column(String(4000))
    title = Column(Text, server_default=text("NULL"))
    description = Column(String(16000), server_default=text("NULL"))
    scope = Column(String(45), nullable=False)
    material = Column(String(45), nullable=False)
    selection = Column(String(45), server_default=text("'other'"))
    type = Column(String(45), nullable=False, server_default=text("'Umbrella'"))
    secondary_study_id = Column(String(45))
    hold_date = Column(Date)
    source_type = Column(String(10), nullable=False, server_default=text("'Germline'"))
    project_accession_code = Column(BigInteger, unique=True, autoincrement=True)
                                    # server_default=text("nextval('project_accession_code_seq'::regclass)"))
    eva_description = Column(String(4000))
    eva_center_name = Column(String(4000))
    eva_submitter_link = Column(String(4000))
    eva_study_accession = Column(BigInteger)
    ena_status = Column(Integer, server_default=text("4"))
    eva_status = Column(Integer, server_default=text("1"))
    ena_timestamp = Column(DateTime)
    eva_timestamp = Column(DateTime(True))
    study_type = Column(String(100))

    url_links = relationship('UrlLink', secondary='project_url_link')


# NOT USED
class ProjectAttribute(Project):
    __tablename__ = 'project_attribute'
    __table_args__ = {'comment': 'Extensible store of attributes for the PROJECT object'}

    project_accession = Column(ForeignKey('project.project_accession'), primary_key=True, index=True)
    value = Column(String(45), nullable=False)


class ProjectSamplesTemp1(Project):
    __tablename__ = 'project_samples_temp1'

    project_accession = Column(ForeignKey('project.project_accession'), primary_key=True)
    sample_count = Column(Integer)
    pro_samp1_id = Column(Integer, nullable=False, autoincrement=True)
    # server_default=text("nextval('pro_samp1_seq'::regclass)"))


# NOT loaded in load_from_ena_postgres_or_file
# some data present in EVAPRO
t_project_counts = Table(
    'project_counts', metadata,
    Column('project_accession', String(15), nullable=False),
    Column('etl_count', BigInteger),
    Column('estimate_count', BigInteger)
)


class ProjectEnaSubmission(Base):
    __tablename__ = 'project_ena_submission'
    __table_args__ = {'comment': 'Links the project with the submission'}

    project_accession = Column(String(45), primary_key=True, nullable=False, index=True)
    submission_id = Column(Integer, primary_key=True, nullable=False, index=True)


class ProjectEvaSubmission(Base):
    __tablename__ = 'project_eva_submission'

    project_accession = Column(String(25), primary_key=True, nullable=False, unique=True)
    old_ticket_id = Column(Integer, primary_key=True, nullable=False)
    eload_id = Column(Integer)
    old_eva_submission_id = Column(Integer)

# NOT USED ANYMORE
t_project_resource = Table(
    'project_resource', metadata,
    Column('project_accession', String(45), nullable=False),
    Column('resource', String(250), nullable=False)
)

# NOT USED ANYMORE
class ProjectVarAccession(Base):
    __tablename__ = 'project_var_accession'

    project_accession_code = Column(BigInteger, primary_key=True)
    last_used_accession_decimal = Column(BigInteger, nullable=False, server_default=text("0"))
    project_prefix = Column(String(6))
    last_used_accession = Column(String(8))


class RemappingProgres(Base):
    __tablename__ = 'remapping_progress'

    source = Column(Text, primary_key=True, nullable=False)
    taxid = Column(Integer, primary_key=True, nullable=False)
    scientific_name = Column(Text)
    assembly_accession = Column(Text, primary_key=True, nullable=False)
    number_of_study = Column(Integer, nullable=False)
    number_submitted_variants = Column(BigInteger, nullable=False)
    release_number = Column(Integer, primary_key=True, nullable=False)
    target_assembly_accession = Column(Text)
    report_time = Column(DateTime, default=func.now())
    progress_status = Column(Text)
    start_time = Column(DateTime)
    completion_time = Column(DateTime)
    remapping_version = Column(Text)
    nb_variant_extracted = Column(Integer)
    nb_variant_remapped = Column(Integer)
    nb_variant_ingested = Column(Integer)


class Sampleset(Base):
    __tablename__ = 'sampleset'
    __table_args__ = {'comment': 'Submitter defined SAMPLESET'}

    sampleset_id = Column(Integer, primary_key=True, autoincrement=True)
                          # server_default=text("nextval('sampleset_sampleset_id_seq'::regclass)"))
    submitter_sampleset_id = Column(String(45), nullable=False)
    description = Column(String(45), nullable=False)
    size = Column(String(45), nullable=False)
    population = Column(String(45), server_default=text("NULL"))
    phenotype = Column(String(45), server_default=text("NULL"))


class Submission(Base):
    __tablename__ = 'submission'
    __table_args__ = {'comment': 'Tracks EVA referred ENA Submissions'}

    submission_id = Column(Integer, primary_key=True, autoincrement=True)
                           # server_default=text("nextval('submission_submission_id_seq'::regclass)"))
    submission_accession = Column(String(45), nullable=False, unique=True)
    type = Column(String(45), nullable=False)
    action = Column(String(45), nullable=False)
    title = Column(String(1000), server_default=text("NULL"))
    notes = Column(String(1000), server_default=text("NULL"))
    date = Column(DateTime, nullable=False, default=func.now())
    brokered = Column(SmallInteger, nullable=False, default=0)


t_supported_assembly_tracker = Table(
    'supported_assembly_tracker', metadata,
    Column('taxonomy_id', Integer, nullable=False),
    Column('source', String(50), nullable=False),
    Column('assembly_id', String(25), nullable=False),
    Column('current', Boolean, nullable=False),
    Column('start_date', Date, nullable=False, default=func.now()),
    Column('end_date', Date, nullable=False)
)


class Taxonomy(Base):
    __tablename__ = 'taxonomy'
    __table_args__ = {'comment': 'Taxonomy IDs of species recorded by EVA'}

    taxonomy_id = Column(Integer, primary_key=True)
    common_name = Column(String(45), server_default=text("NULL"))
    scientific_name = Column(String(255), nullable=False, server_default=text("NULL"))
    taxonomy_code = Column(String(100))
    eva_name = Column(String(40))


t_taxonomy_example = Table(
    'taxonomy_example', metadata,
    Column('taxonomy_ids', BigInteger),
    Column('taxonomy_common_names', Text),
    Column('taxonomy_scientific_names', Text)
)


class UrlLink(Base):
    __tablename__ = 'url_link'
    __table_args__ = {'comment': 'Table linking to external urls'}

    url_link_id = Column(Integer, primary_key=True, autoincrement=True)
                         # server_default=text("nextval('url_link_url_link_id_seq'::regclass)"))
    url = Column(CHAR(1), nullable=False)
    label = Column(CHAR(1), server_default=text("NULL"))


class AssemblySet(Base):
    __tablename__ = 'assembly_set'
    __table_args__ = (
        UniqueConstraint('taxonomy_id', 'assembly_name'),
    )

    assembly_set_id = Column(Integer, primary_key=True, autoincrement=True)
                             # server_default=text("nextval('assembly_set_id_seq'::regclass)"))
    taxonomy_id = Column(ForeignKey('taxonomy.taxonomy_id'), nullable=False)
    assembly_name = Column(String(64))
    assembly_code = Column(String(64))

    taxonomy = relationship('Taxonomy')


t_display_experiment_type = Table(
    'display_experiment_type', metadata,
    Column('experiment_type', ForeignKey('experiment_cv.experiment_type'), nullable=False),
    Column('display_type', String(45), nullable=False)
)


class EvaSubmission(Base):
    __tablename__ = 'eva_submission'

    eva_submission_id = Column(Integer, primary_key=True, autoincrement=True)
                               # server_default=text("nextval('eva_submission_eva_submission_id_seq'::regclass)"))
    eva_submission_status_id = Column(ForeignKey('eva_submission_status_cv.eva_submission_status_id', match='FULL'),
                                      nullable=False)
    hold_date = Column(Date)

    eva_submission_status = relationship('EvaSubmissionStatusCv')


class File(Base):
    __tablename__ = 'file'
    __table_args__ = (
        Index('file_ids_name_idx', 'file_id', 'ena_submission_file_id', 'filename', unique=True),
        {'comment': 'Stores the files'}
    )

    file_id = Column(Integer, primary_key=True, autoincrement=True)
                     # server_default=text("nextval('file_file_id_seq'::regclass)"))
    ena_submission_file_id = Column(String(45), server_default=text("NULL"))
    filename = Column(String(250), nullable=False, index=True)
    file_md5 = Column(String(250), nullable=False)
    file_location = Column(String(250), server_default=text("NULL"))
    file_type = Column(String(250), nullable=False)
    file_class = Column(ForeignKey('file_class_cv.file_class', match='FULL'), nullable=False)
    file_version = Column(Integer, nullable=False)
    is_current = Column(SmallInteger, nullable=False)
    ftp_file = Column(String(250), server_default=text("NULL"))
    mongo_load_status = Column(SmallInteger, nullable=False, server_default=text("0"))
    eva_submission_file_id = Column(String(15))

    file_class_cv = relationship('FileClassCv')
    samples = relationship('Sample', secondary='sample_file')


class ProjectDbxref(Base):
    __tablename__ = 'project_dbxref'
    __table_args__ = {'comment': 'Table linking project to a database '}

    project_accession = Column(String(45), primary_key=True, nullable=False, index=True)
    dbxref_id = Column(ForeignKey('dbxref.dbxref_id'), primary_key=True, nullable=False, index=True)

    dbxref = relationship('Dbxref')


class ProjectTaxonomy(Base):
    __tablename__ = 'project_taxonomy'
    __table_args__ = {'comment': 'Table with the taxonomy of a project'}

    project_accession = Column(ForeignKey('project.project_accession', match='FULL'), primary_key=True, nullable=False,
                               index=True)
    taxonomy_id = Column(ForeignKey('taxonomy.taxonomy_id', match='FULL'), primary_key=True, nullable=False, index=True)

    project = relationship('Project')
    taxonomy = relationship('Taxonomy')


t_project_url_link = Table(
    'project_url_link', metadata,
    Column('project_accession', ForeignKey('project.project_accession'), primary_key=True, nullable=False, index=True),
    Column('url_link_id', ForeignKey('url_link.url_link_id', match='FULL'), primary_key=True, nullable=False,
           index=True),
    comment='Links a project to URL links'
)


class Sample(Base):
    __tablename__ = 'sample'
    __table_args__ = {'comment': 'Samples referenced by EVA as defined by the submitter (until'}

    sample_id = Column(Integer, primary_key=True, autoincrement=True)
                       # server_default=text("nextval('sample_sample_id_seq'::regclass)"))
    gender = Column(String(45), nullable=False)
    phenotype = Column(String(45), nullable=False)
    taxonomy_id = Column(ForeignKey('taxonomy.taxonomy_id', match='FULL'), nullable=False, index=True)
    title = Column(String(250), nullable=False)
    description = Column(CHAR(1), server_default=text("NULL"))
    sample_type = Column(String(250), server_default=text("NULL"))
    subject = Column(String(250), server_default=text("NULL"))
    disease_site = Column(String(250), server_default=text("NULL"))
    strain = Column(String(250), server_default=text("NULL"))

    taxonomy = relationship('Taxonomy')
    url_links = relationship('UrlLink', secondary='sample_url_link')
    samplesets = relationship('Sampleset', secondary='sample_sampleset')


class EvaReferencedSample(Sample):
    __tablename__ = 'eva_referenced_sample'
    __table_args__ = {'comment': 'Lists all samples referenced by EVA files'}

    sample_id = Column(ForeignKey('sample.sample_id', match='FULL'), primary_key=True, index=True, autoincrement=True)
                       # server_default=text("nextval('eva_referenced_sample_sample_id_seq'::regclass)"))
    sample_accession = Column(String(45), nullable=False)
    sample_label = Column(String(45), server_default=text("NULL"))


class AccessionedAssembly(Base):
    __tablename__ = 'accessioned_assembly'

    assembly_set_id = Column(ForeignKey('assembly_set.assembly_set_id'), primary_key=True, nullable=False)
    assembly_accession = Column(String(25), primary_key=True, nullable=False)
    assembly_chain = Column(String(25), nullable=False)
    assembly_version = Column(Integer, nullable=False)

    assembly_set = relationship('AssemblySet')


class Analysis(Base):
    __tablename__ = 'analysis'
    __table_args__ = {'comment': 'Table detailing analyses administered by the EVA'}

    analysis_accession = Column(String(45), primary_key=True)
    title = Column(String(1000), nullable=False)
    alias = Column(String(1000), nullable=False)
    description = Column(String(12000), server_default=text("NULL"))
    center_name = Column(String(500), server_default=text("NULL"))
    date = Column(DateTime)
    vcf_reference = Column(String(250))
    vcf_reference_accession = Column(String(25))
    hidden_in_eva = Column(Integer, server_default=text("0"))
    assembly_set_id = Column(ForeignKey('assembly_set.assembly_set_id'))

    assembly_set = relationship('AssemblySet')
    calling_algorithms = relationship('CallingAlgorithm', secondary='analysis_calling_algorithm')
    files = relationship('File', secondary='analysis_file')
    platforms = relationship('Platform', secondary='analysis_platform')
    sequences = relationship('EvaReferencedSequence', secondary='analysis_sequence')
    submissions = relationship('Submission', secondary='analysis_submission', backref='analyses')
    experiment_types = relationship('ExperimentType', secondary='analysis_experiment_type')
    project = relationship('Project', secondary='project_analysis')


class AnalysisAttribute(Analysis):
    __tablename__ = 'analysis_attribute'
    __table_args__ = {'comment': 'Extensible store of attributes for ANALYSIS'}

    analysis_accession = Column(ForeignKey('analysis.analysis_accession'), primary_key=True, index=True)
    value = Column(String(250), nullable=False)


class BrowsableFile(Base):
    __tablename__ = 'browsable_file'
    __table_args__ = (
        Index('project_file_release_unique_idx', 'project_accession', 'filename', 'eva_release', unique=True),
    )

    file_id = Column(ForeignKey('file.file_id'), primary_key=True, nullable=False)
    ena_submission_file_id = Column(String(45))
    filename = Column(String(250), nullable=False, index=True)
    loaded = Column(Boolean, primary_key=True, nullable=False, server_default=text("false"))
    eva_release = Column(String(50), primary_key=True, nullable=False,
                         server_default=text("'Unreleased'"))
    deleted = Column(Boolean, nullable=False, server_default=text("false"))
    eva_release_deleted = Column(String(50), nullable=False, server_default=text("'None'"))
    project_accession = Column(String(25))
    loaded_assembly = Column(String(500))
    assembly_set_id = Column(ForeignKey('assembly_set.assembly_set_id'))

    assembly_set = relationship('AssemblySet')
    file = relationship('File')


class CustomAssembly(Base):
    __tablename__ = 'custom_assembly'

    assembly_set_id = Column(ForeignKey('assembly_set.assembly_set_id'), nullable=False, unique=True)
    assembly_location = Column(String(250), primary_key=True, nullable=False)
    assembly_file_name = Column(String(250), primary_key=True, nullable=False)

    assembly_set = relationship('AssemblySet', uselist=False)


t_dbsnp_assemblies_b4_eva1376 = Table(
    'dbsnp_assemblies_b4_eva1376', metadata,
    Column('database_name', String(50), nullable=False),
    Column('assembly_set_id', ForeignKey('assembly_set.assembly_set_id')),
    Column('loaded', Boolean, server_default=text("false")),
    UniqueConstraint('database_name', 'assembly_set_id')
)

t_dbsnp_submission_status = Table(
    'dbsnp_submission_status', metadata,
    Column('dbsnp_status_id', ForeignKey('dbsnp_status_cv.dbsnp_status_id'), nullable=False),
    Column('eva_submission_id', ForeignKey('eva_submission.eva_submission_id'), nullable=False, unique=True)
)


class FileFilterTag(Base):
    __tablename__ = 'file_filter_tag'
    __table_args__ = {'comment': 'Links the FILTER tags used in a specific file'}

    file_id = Column(ForeignKey('file.file_id'), primary_key=True, nullable=False, index=True)
    filter_tag_id = Column(Integer, primary_key=True, nullable=False, index=True)

    file = relationship('File')


class FileFormatTag(Base):
    __tablename__ = 'file_format_tag'
    __table_args__ = {'comment': 'Links the FILE to the FORMAT tags used within the file'}

    file_id = Column(ForeignKey('file.file_id'), primary_key=True, nullable=False, index=True)
    format_tag_id = Column(Integer, primary_key=True, nullable=False, index=True)

    file = relationship('File')


t_sample_dbxref = Table(
    'sample_dbxref', metadata,
    Column('sample_id', ForeignKey('sample.sample_id'), primary_key=True, nullable=False, index=True),
    Column('dbxref_id', ForeignKey('dbxref.dbxref_id'), primary_key=True, nullable=False, index=True),
    comment='Links SAMPLE to a DBXREF(s)'
)

t_sample_file = Table(
    'sample_file', metadata,
    Column('sample_id', ForeignKey('sample.sample_id'), primary_key=True, nullable=False, index=True),
    Column('file_id', ForeignKey('file.file_id'), primary_key=True, nullable=False, index=True),
    comment='Links SAMPLES to a FILE'
)

t_sample_sampleset = Table(
    'sample_sampleset', metadata,
    Column('sample_id', ForeignKey('sample.sample_id'), primary_key=True, nullable=False, index=True),
    Column('sampleset_id', ForeignKey('sampleset.sampleset_id'), primary_key=True, nullable=False, index=True),
    comment='SAMPLE to SAMPLESET linker table'
)

t_sample_url_link = Table(
    'sample_url_link', metadata,
    Column('sample_id', ForeignKey('sample.sample_id'), primary_key=True, nullable=False, index=True),
    Column('url_link_id', ForeignKey('url_link.url_link_id', match='FULL'), primary_key=True, nullable=False,
           index=True),
    comment='Links SAMPLE to URL_LINK'
)

t_analysis_calling_algorithm = Table(
    'analysis_calling_algorithm', metadata,
    Column('analysis_accession', ForeignKey('analysis.analysis_accession'), primary_key=True, nullable=False,
           index=True),
    Column('calling_algorithm_id', ForeignKey('calling_algorithm.calling_algorithm_id', match='FULL'), primary_key=True,
           nullable=False, index=True),
    comment='Table to link ANALYSIS with CallingAlgorithm(s)'
)


class AnalysisExperiment(Base):
    __tablename__ = 'analysis_experiment'
    __table_args__ = {'comment': 'Links ANALYSIS to ENA EXPERIMENT'}

    anaysis_accession = Column(ForeignKey('analysis.analysis_accession'), primary_key=True, nullable=False, index=True)
    experiment_accession = Column(String(45), primary_key=True, nullable=False)

    analysis = relationship('Analysis')


t_analysis_experiment_type = Table(
    'analysis_experiment_type', metadata,
    Column('analysis_accession', ForeignKey('analysis.analysis_accession'), primary_key=True, nullable=False,
           index=True),
    Column('experiment_type_id', ForeignKey('experiment_type.experiment_type_id', match='FULL'), primary_key=True,
           nullable=False, index=True),
    comment='ANALYSIS to EXPERIMENT_TYPE linker table'
)

t_analysis_file = Table(
    'analysis_file', metadata,
    Column('analysis_accession', ForeignKey('analysis.analysis_accession'), primary_key=True, nullable=False,
           index=True),
    Column('file_id', ForeignKey('file.file_id', match='FULL'), primary_key=True, nullable=False, index=True),
    Index('analfile_analfile_idx', 'analysis_accession', 'file_id', unique=True),
    comment='Table determining which file is associated with which analys'
)

t_analysis_platform = Table(
    'analysis_platform', metadata,
    Column('analysis_accession', ForeignKey('analysis.analysis_accession'), primary_key=True, nullable=False,
           index=True),
    Column('platform_id', ForeignKey('platform.platform_id', match='FULL'), primary_key=True, nullable=False,
           index=True),
    comment='ANALYSIS to PLATFORM linker table'
)

t_analysis_sequence = Table(
    'analysis_sequence', metadata,
    Column('analysis_accession', ForeignKey('analysis.analysis_accession'), primary_key=True, nullable=False,
           index=True),
    Column('sequence_id', ForeignKey('eva_referenced_sequence.sequence_id', match='FULL'), primary_key=True,
           nullable=False, index=True),
    comment='Links an analysis to a sequence(s)'
)

t_analysis_submission = Table(
    'analysis_submission', metadata,
    Column('analysis_accession', ForeignKey('analysis.analysis_accession'), primary_key=True, nullable=False,
           index=True),
    Column('submission_id', ForeignKey('submission.submission_id', match='FULL'), primary_key=True, nullable=False,
           index=True),
    comment='Links Analysis with the associated submission(s)'
)

t_dbsnp_assemblies = Table(
    'dbsnp_assemblies', metadata,
    Column('database_name', String(50), nullable=False),
    Column('assembly_set_id', Integer),
    Column('assembly_accession', String(25)),
    Column('loaded', Boolean, server_default=text("false")),
    ForeignKeyConstraint(['assembly_set_id', 'assembly_accession'],
                         ['accessioned_assembly.assembly_set_id', 'accessioned_assembly.assembly_accession']),
    UniqueConstraint('database_name', 'assembly_set_id', 'assembly_accession')
)

t_project_analysis = Table(
    'project_analysis', metadata,
    Column('project_accession', ForeignKey('project.project_accession'), primary_key=True, nullable=False, index=True),
    Column('analysis_accession', ForeignKey('analysis.analysis_accession'), primary_key=True, nullable=False,
           index=True),
    Index('projanal_analproj_idx', 'project_accession', 'analysis_accession', unique=True),
    comment='Table assigning an ENA Analysis object to a project'
)

# MATERIALIZED VIEW
t_dgva_organism_mv = Table(
    'dgva_organism_mv', metadata,
    Column('organism_common_name', Text),
    Column('count', BigInteger)
)

t_dgva_study_mv = Table(
    'dgva_study_mv', metadata,
    Column('study_accession', String(15)),
    Column('call_count', Integer),
    Column('region_count', Integer),
    Column('variant_count', Integer),
    Column('tax_id', Text),
    Column('common_name', Text),
    Column('scientific_name', Text),
    Column('pubmed_id', Text),
    Column('alias', Text),
    Column('display_name', Text),
    Column('study_type', String(100)),
    Column('project_id', String(100)),
    Column('study_url', String(4000)),
    Column('study_description', String(4000)),
    Column('assembly_name', String(50))
)

t_dgva_study_species = Table(
    'dgva_study_species', metadata,
    Column('study_accession', String(15)),
    Column('eva_species_name', String(40))
)

t_dgva_study_taxonomy_mv = Table(
    'dgva_study_taxonomy_mv', metadata,
    Column('study_accession', String(15)),
    Column('taxonomy_id', Integer)
)

t_study_browser = Table(
    'study_browser', metadata,
    Column('project_accession', String(45)),
    Column('study_id', BigInteger),
    Column('project_title', Text),
    Column('description', String),
    Column('tax_id', Text),
    Column('common_name', Text),
    Column('scientific_name', Text),
    Column('source_type', String),
    Column('study_type', String),
    Column('variant_count', BigInteger),
    Column('samples', Integer),
    Column('center', String),
    Column('scope', String),
    Column('material', String),
    Column('publications', Text),
    Column('associated_projects', Text),
    Column('experiment_type', Text),
    Column('experiment_type_abbreviation', Text),
    Column('assembly_accession', Text),
    Column('assembly_name', Text),
    Column('platform', Text),
    Column('resource', String),
    Column('browsable', Boolean)
)

t_study_browser_updated_2019_07_09 = Table(
    'study_browser_updated_2019_07_09', metadata,
    Column('project_accession', String(45)),
    Column('study_id', BigInteger),
    Column('project_title', Text),
    Column('description', String),
    Column('tax_id', Text),
    Column('common_name', Text),
    Column('scientific_name', Text),
    Column('source_type', String),
    Column('study_type', String),
    Column('variant_count', BigInteger),
    Column('samples', Integer),
    Column('center', String),
    Column('scope', String),
    Column('material', String),
    Column('publications', Text),
    Column('associated_projects', Text),
    Column('experiment_type', Text),
    Column('experiment_type_abbreviation', Text),
    Column('assembly_accession', Text),
    Column('assembly_name', Text),
    Column('platform', Text),
    Column('resource', String),
    Column('browsable', Boolean)
)

# VIEWS

t_assembly = Table(
    'assembly', metadata,
    Column('assembly_accession', String(25)),
    Column('assembly_chain', String(25)),
    Column('assembly_version', Integer),
    Column('assembly_set_id', Integer),
    Column('assembly_name', String(64)),
    Column('assembly_code', String(64)),
    Column('taxonomy_id', Integer),
    Column('assembly_location', String(250)),
    Column('assembly_filename', String(250)),
    Column('assembly_in_accessioning_store', Boolean)
)

t_browsable_file_view = Table(
    'browsable_file_view', metadata,
    Column('file_id', Integer),
    Column('ena_submission_file_id', String(45)),
    Column('filename', String(250)),
    Column('eva_release', String),
    Column('loaded', Boolean),
    Column('to_delete', Boolean),
    Column('project_accession', String(45)),
    Column('loaded_assembly', String(25)),
    Column('assembly_set_id', Integer)
)

t_dgva_organism = Table(
    'dgva_organism', metadata,
    Column('organism_common_name', Text),
    Column('count', BigInteger)
)

t_dgva_study_vw = Table(
    'dgva_study_vw', metadata,
    Column('study_accession', String(15)),
    Column('call_count', Integer),
    Column('region_count', Integer),
    Column('variant_count', Integer),
    Column('tax_id', Text),
    Column('common_name', Text),
    Column('scientific_name', Text),
    Column('pubmed_id', Text),
    Column('alias', Text),
    Column('display_name', Text),
    Column('study_type', String(100)),
    Column('project_id', String(100)),
    Column('study_url', String(4000)),
    Column('study_description', String(4000)),
    Column('assembly_name', String(50))
)

t_ena_analysis_sample = Table(
    'ena_analysis_sample', metadata,
    Column('analysis_id', String(15)),
    Column('sample_id', String(15)),
    Column('audit_time', Date),
    Column('audit_user', String(30)),
    Column('audit_osuser', String(30))
)

t_ena_link = Table(
    'ena_link', metadata,
    Column('child', String(15)),
    Column('parent', String(15)),
    Column('role_id', Integer)
)

t_ena_project_analysis = Table(
    'ena_project_analysis', metadata,
    Column('reference_accession', String(25)),
    Column('resource', Text)
)

t_eva_organism_mv = Table(
    'eva_organism_mv', metadata,
    Column('organism_common_name', String),
    Column('project_count', BigInteger)
)

t_eva_organism_vw = Table(
    'eva_organism_vw', metadata,
    Column('organism_common_name', String),
    Column('project_count', BigInteger)
)

t_eva_submission_status = Table(
    'eva_submission_status', metadata,
    Column('eva_ticket', Text),
    Column('project_accession', String),
    Column('alias', String(4000)),
    Column('child_projects', Text),
    Column('taxonomy_ids', Text),
    Column('taxonomy_common_names', Text),
    Column('submission_status', String(500)),
    Column('description', String(1000)),
    Column('dbsnp_submission_status', String(500)),
    Column('dbsnp_status_description', String(1000))
)

t_ftp_files_vw = Table(
    'ftp_files_vw', metadata,
    Column('project_accession', String(45)),
    Column('analysis_accession', String(45)),
    Column('filename', String(250)),
    Column('file_md5', String(250)),
    Column('file_location', String(250))
)

t_project_children_taxonomy = Table(
    'project_children_taxonomy', metadata,
    Column('project_accession', String(25)),
    Column('child_projects', Text),
    Column('taxonomy_ids', Text),
    Column('taxonomy_common_names', Text),
    Column('taxonomy_scientific_names', Text)
)

t_project_children_taxonomy_j = Table(
    'project_children_taxonomy_j', metadata,
    Column('project_accession', String(25)),
    Column('child_projects', JSON),
    Column('taxonomy_ids', JSON),
    Column('taxonomy_common_names', JSON),
    Column('taxonomy_scientific_names', JSON)
)

t_project_experiment = Table(
    'project_experiment', metadata,
    Column('project_accession', String(45)),
    Column('experiment_type', Text),
    Column('display_type', Text),
    Column('experiment_type_abbreviation', Text)
)

t_project_experiment_j = Table(
    'project_experiment_j', metadata,
    Column('project_accession', String(45)),
    Column('experiment_type', JSON)
)

t_project_platform = Table(
    'project_platform', metadata,
    Column('project_accession', String(45)),
    Column('platform', String(4000))
)

t_project_publication = Table(
    'project_publication', metadata,
    Column('project_accession', String(45)),
    Column('db', String(45)),
    Column('id', String(45))
)

t_project_reference = Table(
    'project_reference', metadata,
    Column('project_accession', String(45)),
    Column('reference_accession', String(25)),
    Column('reference_name', String(250))
)

t_project_submitted_var_counts = Table(
    'project_submitted_var_counts', metadata,
    Column('project_accession', String(45)),
    Column('last_used_accession_decimal', BigInteger),
    Column('taxonomy_id', Integer)
)

t_species_submitted_var_counts = Table(
    'species_submitted_var_counts', metadata,
    Column('taxonomy_id', Integer),
    Column('num_submitted_vars', Numeric)
)

t_study_browser_json = Table(
    'study_browser_json', metadata,
    Column('json', JSON)
)

t_study_browser_nofilt_vw = Table(
    'study_browser_nofilt_vw', metadata,
    Column('project_accession', String(45)),
    Column('study_id', BigInteger),
    Column('project_title', Text),
    Column('description', String),
    Column('tax_id', Text),
    Column('common_name', Text),
    Column('scientific_name', Text),
    Column('source_type', String),
    Column('study_type', String),
    Column('variant_count', BigInteger),
    Column('samples', Integer),
    Column('center', String),
    Column('scope', String),
    Column('material', String),
    Column('publications', Text),
    Column('associated_projects', Text),
    Column('experiment_type', Text),
    Column('experiment_type_abbreviation', Text),
    Column('assembly_accession', Text),
    Column('assembly_name', Text),
    Column('platform', Text),
    Column('resource', String),
    Column('eva_submission_status_id', Integer),
    Column('eva_submission_id', Integer),
    Column('old_eva_submission_id', Integer),
    Column('eload_id', Integer),
    Column('eva_status', Integer),
    Column('ena_status', Integer)
)

t_study_browser_vw = Table(
    'study_browser_vw', metadata,
    Column('project_accession', String(45)),
    Column('study_id', BigInteger),
    Column('project_title', Text),
    Column('description', String),
    Column('tax_id', Text),
    Column('common_name', Text),
    Column('scientific_name', Text),
    Column('source_type', String),
    Column('study_type', String),
    Column('variant_count', BigInteger),
    Column('samples', Integer),
    Column('center', String),
    Column('scope', String),
    Column('material', String),
    Column('publications', Text),
    Column('associated_projects', Text),
    Column('experiment_type', Text),
    Column('experiment_type_abbreviation', Text),
    Column('assembly_accession', Text),
    Column('assembly_name', Text),
    Column('platform', Text),
    Column('resource', String)
)

t_vw_browsable_file_view = Table(
    'vw_browsable_file_view', metadata,
    Column('file_id', Integer),
    Column('ena_submission_file_id', String(45)),
    Column('filename', String(250)),
    Column('file_type', String(250)),
    Column('file_version', Integer),
    Column('is_current', SmallInteger),
    Column('file_location', String(250)),
    Column('loaded', Boolean),
    Column('to_delete', Boolean),
    Column('project_accession', String(45)),
    Column('loaded_assembly', String),
    Column('assembly_set_id', Integer)
)

t_vw_tmp_ftp_file = Table(
    'vw_tmp_ftp_file', metadata,
    Column('project_accession', String(25)),
    Column('filename', String(250)),
    Column('file_id', Integer),
    Column('analysis_accession', String(45)),
    Column('ftp_file', Text)
)
