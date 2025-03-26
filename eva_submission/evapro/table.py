# coding: utf-8
from sqlalchemy import BigInteger, Boolean, CHAR, Column, Date, DateTime, ForeignKey, Index, \
    Integer, SmallInteger, String, Table, Text, UniqueConstraint, text, func
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata

class Dbxref(Base):
    __tablename__ = 'dbxref'
    __table_args__ = (
        UniqueConstraint('db', 'id'),
        {'comment': 'List of DBXREFs'}
    )

    dbxref_id = Column(Integer, primary_key=True, autoincrement=True)
    db = Column(String(45), nullable=False)
    id = Column(String(45), nullable=False)
    label = Column(String(250), server_default=text("NULL"))
    link_type = Column(String(100), nullable=False)
    source_object = Column(String(100), nullable=False)

    samples = relationship('Sample', secondary='sample_dbxref')

class EvaReferencedSequence(Base):
    __tablename__ = 'eva_referenced_sequence'
    __table_args__ = {'comment': 'Sequences referenced by EVA '}

    sequence_id = Column(Integer, primary_key=True, autoincrement=True)
    sequence_accession = Column(String(45), nullable=False)
    label = Column(String(45), server_default=text("NULL"))
    ref_name = Column(String(45), server_default=text("NULL"))


class EvaSubmissionStatusCv(Base):
    __tablename__ = 'eva_submission_status_cv'

    eva_submission_status_id = Column(Integer, primary_key=True)
    submission_status = Column(String(500), nullable=False)
    description = Column(String(1000))



class ExperimentType(Base):
    __tablename__ = 'experiment_type'
    __table_args__ = {'comment': 'Experiment type'}

    experiment_type_id = Column(Integer, primary_key=True, autoincrement=True)
    experiment_type = Column(String(45), nullable=False)


class FileClassCv(Base):
    __tablename__ = 'file_class_cv'
    __table_args__ = {'comment': 'List of the file classes EVA stores'}

    file_class_id = Column(Integer, primary_key=True, autoincrement=True)
    file_class = Column(String(45), nullable=False, unique=True)


class LinkedProject(Base):
    __tablename__ = 'linked_project'
    __table_args__ = {'comment': 'Table allowing a project to link to another project'}

    linked_project_id = Column(Integer, primary_key=True, autoincrement=True)
    project_accession = Column(String(45), nullable=False, index=True)
    linked_project_accession = Column(String(45), nullable=False)
    linked_project_relation = Column(String(45), nullable=False)

    link_live_for_eva = Column(Boolean, server_default=text("false"))


class Platform(Base):
    __tablename__ = 'platform'
    __table_args__ = {'comment': 'Platform referenced by EVA'}

    platform_id = Column(Integer, primary_key=True, autoincrement=True)
    platform = Column(String(4000), nullable=False)
    manufacturer = Column(String(100))

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
    taxonomies = relationship('Taxonomy', secondary='project_taxonomy', back_populates='projects')
    analyses = relationship('Analysis', secondary='project_analysis', backref='projects')

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

class Sampleset(Base):
    __tablename__ = 'sampleset'
    __table_args__ = {'comment': 'Submitter defined SAMPLESET'}

    sampleset_id = Column(Integer, primary_key=True, autoincrement=True)
    submitter_sampleset_id = Column(String(45), nullable=False)
    description = Column(String(45), nullable=False)
    size = Column(String(45), nullable=False)
    population = Column(String(45), server_default=text("NULL"))
    phenotype = Column(String(45), server_default=text("NULL"))


class Submission(Base):
    __tablename__ = 'submission'
    __table_args__ = {'comment': 'Tracks EVA referred ENA Submissions'}

    submission_id = Column(Integer, primary_key=True, autoincrement=True)
    submission_accession = Column(String(45), nullable=False, unique=True)
    type = Column(String(45), nullable=False)
    action = Column(String(45), nullable=False)
    title = Column(String(1000), server_default=text("NULL"))
    notes = Column(String(1000), server_default=text("NULL"))
    date = Column(DateTime, nullable=False, default=func.now())
    brokered = Column(SmallInteger, nullable=False, default=0)


class Taxonomy(Base):
    __tablename__ = 'taxonomy'
    __table_args__ = {'comment': 'Taxonomy IDs of species recorded by EVA'}

    taxonomy_id = Column(Integer, primary_key=True)
    common_name = Column(String(45), server_default=text("NULL"))
    scientific_name = Column(String(255), nullable=False, server_default=text("NULL"))
    taxonomy_code = Column(String(100))
    eva_name = Column(String(40))
    projects = relationship('Project', secondary='project_taxonomy', )


class UrlLink(Base):
    __tablename__ = 'url_link'
    __table_args__ = {'comment': 'Table linking to external urls'}

    url_link_id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(CHAR(1), nullable=False)
    label = Column(CHAR(1), server_default=text("NULL"))


class AssemblySet(Base):
    __tablename__ = 'assembly_set'
    __table_args__ = (
        UniqueConstraint('taxonomy_id', 'assembly_name'),
    )

    assembly_set_id = Column(Integer, primary_key=True, autoincrement=True)
    taxonomy_id = Column(ForeignKey('taxonomy.taxonomy_id'), nullable=False)
    assembly_name = Column(String(64))
    assembly_code = Column(String(64))

    taxonomy = relationship('Taxonomy')


class EvaSubmission(Base):
    __tablename__ = 'eva_submission'

    eva_submission_id = Column(Integer, primary_key=True, autoincrement=True)
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
    samples = relationship('SampleInFile', back_populates='file')

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



t_project_url_link = Table(
    'project_url_link', metadata,
    Column('project_accession', ForeignKey('project.project_accession'), primary_key=True, nullable=False, index=True),
    Column('url_link_id', ForeignKey('url_link.url_link_id', match='FULL'), primary_key=True, nullable=False,
           index=True),
    comment='Links a project to URL links'
)


class Sample(Base):
    __tablename__ = 'sample'
    __table_args__ = {'comment': 'Samples referenced by EVA as defined by the submitter'}

    sample_id = Column(Integer, primary_key=True, autoincrement=True)
    biosample_accession = Column(String(45), nullable=False)
    ena_accession = Column(String(45), nullable=False)
    files = relationship("SampleInFile", back_populates="sample")


class SampleInFile(Base):
    __tablename__ = 'file_sample'
    __table_args__ = {'comment': 'Links sample to a file'}
    file_id = Column( ForeignKey('file.file_id', match='FULL'), primary_key=True, nullable=False, index=True)
    sample_id = Column(ForeignKey('sample.sample_id', match='FULL'), primary_key=True, nullable=False, index=True)
    name_in_file = Column(String(250), nullable=False)
    sample = relationship("Sample", back_populates="files")
    file = relationship("File", back_populates="samples")


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
    files = relationship('File', secondary='analysis_file', backref='analyses')
    platforms = relationship('Platform', secondary='analysis_platform')
    sequences = relationship('EvaReferencedSequence', secondary='analysis_sequence')
    submissions = relationship('Submission', secondary='analysis_submission', backref='analyses')
    experiment_types = relationship('ExperimentType', secondary='analysis_experiment_type')


class AnalysisAttribute(Base):
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

t_sample_dbxref = Table(
    'sample_dbxref', metadata,
    Column('sample_id', ForeignKey('sample.sample_id'), primary_key=True, nullable=False, index=True),
    Column('dbxref_id', ForeignKey('dbxref.dbxref_id'), primary_key=True, nullable=False, index=True),
    comment='Links SAMPLE to a DBXREF(s)'
)

class AnalysisExperiment(Base):
    __tablename__ = 'analysis_experiment'
    __table_args__ = {'comment': 'Links ANALYSIS to ENA EXPERIMENT'}

    anaysis_accession = Column(ForeignKey('analysis.analysis_accession'), primary_key=True, nullable=False, index=True)
    experiment_accession = Column(String(45), primary_key=True, nullable=False)

    analysis = relationship('Analysis')


class ProjectSampleTemp1(Base):
    __tablename__ = 'project_samples_temp1'

    project_accession = Column(ForeignKey('project.project_accession'), primary_key=True, nullable=False, index=True)
    sample_count = Column(Integer)
    pro_samp1_id = Column(Integer, autoincrement=True)


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

t_project_analysis = Table(
    'project_analysis', metadata,
    Column('project_accession', ForeignKey('project.project_accession'), primary_key=True, nullable=False, index=True),
    Column('analysis_accession', ForeignKey('analysis.analysis_accession'), primary_key=True, nullable=False,
           index=True),
    Index('projanal_analproj_idx', 'project_accession', 'analysis_accession', unique=True),
    comment='Table assigning an ENA Analysis object to a project'
)

# MATERIALIZED VIEW

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
