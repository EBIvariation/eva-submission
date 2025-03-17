#! /usr/bin/perl

use strict;
use warnings;
use DBI;
use Data::Dumper;
use XML::LibXML;
use Getopt::Long qw(:config no_ignore_case);
my $hostname_era;
my $port_era;
my $sid_era;
my $username_era;
my $password_era;
my $hostname_eta;
my $port_eta;
my $sid_eta;
my $username_eta;
my $password_eta;

my ($study_type,$project_accession,$file_class,$file_version,$file_location,
    $analysis_file,$analysis_only,$project_only,$eload,$analysis_accession,
    $project_file,$submission_file,$database,$taxid_eva,$use_sub_file_id,
    $user_accounts_arg, $ena_ftp_file_prefix_path, @user_accounts);

$ena_ftp_file_prefix_path = "/ftp.sra.ebi.ac.uk/vol1";

&GetOptions(
    'project|p=s'           => \$project_accession,
    'file_class|c=s'        => \$file_class,
    'file_version|v=s'      => \$file_version,
    'file_location|l=s'     =>\$file_location,
    'analysis_file|f=s'     => \$analysis_file,
    'analysis_only|A=s'     => \$analysis_only,
    'project_only|P=s'      => \$project_only,
    'eload|e=s' => \$eload,
    'analysis_accession|a=s'    => \$analysis_accession,
    'project_file|F=s'  => \$project_file,
    'submission_file|s=s' => \$submission_file,
    'taxonomy|t=s' => \$taxid_eva,
    'study_type|T=s' => \$study_type,
    'use_sub_file_id|S' => \$use_sub_file_id,
    'user_accounts|u=s' => \$user_accounts_arg,
);

if (defined $user_accounts_arg) {
    @user_accounts = split /,/, $user_accounts_arg;
} else {
    @user_accounts = ("webin-1008");
}
my $sql_placeholder = join ",", ("?") x @user_accounts;

if (defined $project_only){
    $project_only = 1;
} else {
    $project_only = 0;
}
if ($project_only == 0){
    if (!defined $file_class){
        warn "Must define the file_class sent to ENA! 'submitted','eva_brokered','eva_value_added'";
        exit(1);
    }
    if (!defined $eload){
        warn "Must have an ELOAD ID";
        exit(1);
    }
}

if ((!defined $project_accession)&&(!defined $project_file)){
    warn "Must define the project accession from ENA!";
    exit(1);
}
if ((defined $project_file)&&(!defined $submission_file)){
    warn "Submission file must be defined if using project_file!";
    exit(1);
}

if (!defined $study_type){
    warn "Study_type defaulting to Control Set";
    $study_type = 'Control Set';
}

my $sub_file_id_col;
if($use_sub_file_id){
    $sub_file_id_col = "submission_file_id";
}else{
    $sub_file_id_col = "data_file_id";
}

my $study_type_hash = {};
$study_type_hash->{'Control Set'}++;
$study_type_hash->{'Case Set'}++;
$study_type_hash->{'Case Control'}++;
$study_type_hash->{'Aggregate'}++;

if (!defined $study_type_hash->{$study_type}){
    warn $study_type . " is not allowed. Please check";
    exit(1);
}

if (!defined $file_version){
    $file_version = 1;
}
if ($project_only ==0){
    if ($file_class eq 'eva_value_added'){
        if(!defined $file_location){
            warn "Need a file location for the eva_value_added files!";
            exit(1);
        }
    }
}

if (defined $analysis_only){
    $analysis_only = 1;
} else {
    $analysis_only = 0;
}
my $dbase_era = 'ERAREAD';
my $dbase_eta = 'ETAPRO';

open FILE, 'databases.txt' or die ("Cannot open PASSWORD file for database connection!");
while(<FILE>){
    chomp();
    if (m/^$dbase_era/){
        my $t;
        ($t,$hostname_era,$port_era,$sid_era,$username_era,$password_era) = split(/\t/);
    }
    if (m/^$dbase_eta/){
        my $t;
        ($t,$hostname_eta,$port_eta,$sid_eta,$username_eta,$password_eta) = split(/\t/);
    }
}

my $accession_to_id = {};

my $dbh_eva = DBI->connect('DBI:Pg:dbname=metadata;host=perl_postgres;port=5432','root_user','root_pass',{'RaiseError' => 1,'AutoCommit' => 0}) or die "Cannot connect to EVAPRO:$!\n";
my $dbh_eta =  DBI->connect("dbi:Oracle://${hostname_eta}:${port_eta}/${sid_eta}", "$username_eta", "$password_eta", { RaiseError => 1, AutoCommit => 0 } );
$dbh_eta->{LongReadLen} = 128000000;
my $loading_db = 'evapro_test';

my $sth_eva = $dbh_eva->prepare("insert into FILE (FILE_MD5,FILENAME,FILE_TYPE,FILE_LOCATION,FILE_CLASS,FILE_VERSION,IS_CURRENT) values (?,?,?,?,?,?,?)");
my $dbh_ena =  DBI->connect("dbi:Oracle://${hostname_era}:${port_era}/${sid_era}", "$username_era", "$password_era", { RaiseError => 1, AutoCommit => 0 } );
$dbh_ena->{LongReadLen} = 128000000;
my $ena_submission_id;
warn "Loading to " . $loading_db;
eval {
    my $sth_ena = $dbh_ena->prepare(<<sqlend);
select s.study_id, project_id, s.submission_id, p.center_name, p.project_alias, s.study_type, p.first_created, p.project_title, p.tax_id, p.scientific_name, p.common_name, xmltype.getclobval(STUDY_XML) study_xml, xmltype.getclobval(PROJECT_XML) project_xml from era.PROJECT p left outer join era.STUDY s using(project_id) where project_id=?
sqlend
    if (!$analysis_only){
        warn "Loading project data";
        #### Insert into PROJECT table
        if (defined $project_accession){
            $sth_ena->execute($project_accession);
            while (my $row = $sth_ena->fetchrow_hashref()){
                warn "Getting data! ".$row->{'PROJECT_ID'};
                if (defined $row->{'STUDY_XML'}){
                    my $parser = XML::LibXML->new();
                    my $doc;
                    eval{
                        $doc = $parser->parse_string($row->{'STUDY_XML'});
                    };
                    if (!$doc){
                        print "Failed to parse STUDY_XML, cannot determine DESCRIPTION!";
                    }
                    my $root = $doc->getDocumentElement;
                    my $desc_nodes = $root->find('/STUDY_SET/STUDY/DESCRIPTOR/STUDY_DESCRIPTION');
                    if (scalar(@$desc_nodes)){
                        $row->{'DESCRIPTION'} = $desc_nodes->get_node(0)->textContent;
                        $row->{'DESCRIPTION'} =~ s/<\/*p>//ig;
                        delete $row->{'STUDY_XML'};
                    }
                }
                my @publicat;
                if (defined $row->{'PROJECT_XML'}){
                    my $parser = XML::LibXML->new();
                    my $doc;
                    eval{
                        $doc = $parser->parse_string($row->{'PROJECT_XML'});
                    };
                    if (!$doc){
                        print "Failed to parse PROJECT_XML, cannot determine PUBLICATIONS!";
                    }

                    my $root = $doc->getDocumentElement;
                    my @projs = $root->getChildrenByTagName('PROJECT');
                    foreach my $proj(@projs){
                        my @pubss = $proj->getChildrenByTagName('PUBLICATIONS');
                        foreach my $pubs(@pubss){
                            my @pub = $pubs->getChildrenByTagName('PUBLICATION');
                            foreach my $p(@pub){
                                my @pls = $p->getChildrenByTagName('PUBLICATION_LINKS');
                                foreach my $pl(@pls){
                                    my @pzs = $pl->getChildrenByTagName('PUBLICATION_LINK');
                                    foreach my $pz(@pzs){
                                        my @xls = $pz->getChildrenByTagName('XREF_LINK');
                                        foreach my $xl(@xls){
                                            my @dbs = $xl->getChildrenByTagName('DB');
                                            my @ids = $xl->getChildrenByTagName('ID');
                                            for (my $i=0;$i<=$#dbs;$i++){
                                                my $zz->{'db'} = $dbs[$i]->textContent();
                                                $zz->{'id'} = $ids[$i]->textContent();
                                                push(@publicat,$zz);
                                            }
                                        }
                                    }

                                }
                            }
                        }
                    }
                }
                if (($row->{'PROJECT_ID'} =~ /^PRJEB/) &&
                    ((!defined $row->{'PROJECT_ALIAS'}) || ($row->{'PROJECT_ALIAS'} eq "") ) ){
                  print "Projects with prefix PRJEB must have a project alias\n";
                  exit(1);
                }
                $sth_eva = $dbh_eva->prepare("insert into PROJECT (PROJECT_ACCESSION,CENTER_NAME,ALIAS,TITLE,DESCRIPTION,SCOPE,MATERIAL,SELECTION,TYPE,SECONDARY_STUDY_ID,STUDY_TYPE) values (?,?,?,?,?,?,?,?,?,?,?)");
                $sth_eva->execute($row->{'PROJECT_ID'},$row->{'CENTER_NAME'},$row->{'PROJECT_ALIAS'},$row->{'PROJECT_TITLE'},$row->{'DESCRIPTION'},'multi-isolate','DNA','other',$row->{'STUDY_TYPE'},$row->{'STUDY_ID'},$study_type);
                warn $row->{'PROJECT_ID'};
                $sth_eva = $dbh_eva->prepare("select project_accession_code from project");
                $sth_eva->execute();
                while(my @row1 = $sth_eva->fetchrow_array()){
                    $accession_to_id->{'PROJECT_ID'} = $row1[0];
                    my $e = $row1[0] + 1;
                    my $sth2_eva = $dbh_eva->prepare("update project set eva_study_accession=".$e." where project_accession = ?");
                    $sth2_eva->execute($row->{'PROJECT_ID'});
                }
                foreach my $pp(@publicat){
                    $sth_eva = $dbh_eva->prepare("select dbxref_id from dbxref where db=? and id=? and source_object=?");
                    $sth_eva->execute($pp->{'db'},$pp->{'id'},'project');
                    my $dbxref = {};
                    while(my @row1 = $sth_eva->fetchrow_array()){
                        $dbxref->{'dbxref_id'} = $row1[0];
                    }
                    if (!defined $dbxref->{'dbxref_id'}){
                        $sth_eva = $dbh_eva->prepare("insert into dbxref (db,id,link_type,source_object) values (?,?,?,?) returning dbxref_id");
                        $sth_eva->execute($pp->{'db'},$pp->{'id'},'publication','project');
                        $dbxref = $sth_eva->fetchrow_hashref();
                    }
                    my $sth2_eva = $dbh_eva->prepare("insert into project_dbxref(project_accession,dbxref_id) values (?,?)");
                    $sth2_eva->execute($project_accession,$dbxref->{'dbxref_id'});
                }
                if (!defined $row->{'TAX_ID'}) {
                    $row->{'TAX_ID'} = $taxid_eva;
                    my $sth_eta = $dbh_eta->prepare("select distinct tax_id,common_name,scientific_name from ETA.GC_ASSEMBLY_SET where TAX_ID=?");
                    $sth_eta->execute($row->{'TAX_ID'});
                    while (my @row_eta = $sth_eta->fetchrow_array()){
                        $row->{'COMMON_NAME'} = $row_eta[1];
                        $row->{'SCIENTIFIC_NAME'} = $row_eta[2];
                    }
                    if (!defined $row->{'SCIENTIFIC_NAME'}){
                        warn "Cannot determine scientific name for tax_id " . $taxid_eva;
                        exit(1);
                    }
                }

                $sth_eva = $dbh_eva->prepare("select taxonomy_id from TAXONOMY where TAXONOMY_ID=?");
                $sth_eva->execute($row->{'TAX_ID'});
                my $tax_id_in_eva = 0;
                while (my $row1 = $sth_eva->fetchrow_hashref()){
                    if ($row1->{'taxonomy_id'} == $row->{'TAX_ID'}){
                        $tax_id_in_eva++;
                        my $sth_eva2 = $dbh_eva->prepare("insert into PROJECT_TAXONOMY (PROJECT_ACCESSION,TAXONOMY_ID) values (?,?)");
                        $sth_eva2->execute($row->{'PROJECT_ID'},$row->{'TAX_ID'});
                    } else {
                        my $sth_eva = $dbh_eva->prepare("insert into TAXONOMY(TAXONOMY_ID,COMMON_NAME,SCIENTIFIC_NAME) values(?,?,?)");
                        $sth_eva->execute($row->{'TAX_ID'},$row->{'COMMON_NAME'},$row->{'SCIENTIFIC_NAME'});
                        my $sth_eva2 = $dbh_eva->prepare("insert into PROJECT_TAXONOMY (PROJECT_ACCESSION,TAXOMONY_ID) values (?,?)");
                        $sth_eva2->execute($row->{'PROJECT_ID'},$row->{'TAX_ID'});
                    }
                }
                if (!$tax_id_in_eva){
                    my $sth_eva = $dbh_eva->prepare("insert into TAXONOMY(TAXONOMY_ID,COMMON_NAME,SCIENTIFIC_NAME) values(?,?,?)");
                    $sth_eva->execute($row->{'TAX_ID'},$row->{'COMMON_NAME'},$row->{'SCIENTIFIC_NAME'});
                    my $sth_eva2 = $dbh_eva->prepare("insert into PROJECT_TAXONOMY (PROJECT_ACCESSION,TAXONOMY_ID) values (?,?)");
                    $sth_eva2->execute($row->{'PROJECT_ID'},$row->{'TAX_ID'});
                }
            #warn Dumper($row);
            }

            $sth_ena = $dbh_ena->prepare("select to_id from era.ena_link where from_id=?");
            $sth_ena->execute($project_accession);
            while(my $row = $sth_ena->fetchrow_hashref()){
                $sth_eva = $dbh_eva->prepare("insert into LINKED_PROJECT (PROJECT_ACCESSION,LINKED_PROJECT_ACCESSION,LINKED_PROJECT_RELATION) values (?,?,?)");
                $sth_eva->execute($project_accession,$row->{'TO_ID'},'PARENT');
            }


            ### Insert into SUBMISSION table
            $sth_eva = $dbh_eva->prepare("insert into SUBMISSION (SUBMISSION_ACCESSION,ACTION,TITLE,DATE,BROKERED,TYPE) values (?,?,?,?,?,?) returning submission_id");
            $sth_ena = $dbh_ena->prepare(<<sqlend);
select submission.submission_id, xmltype.getclobval(SUBMISSION_XML) submission_xml, submission.last_updated UPDATED from era.submission left outer join era.study on submission.submission_id=study.submission_id where study.project_id=? and study.submission_id like 'ERA%'
sqlend
            $sth_ena->execute($project_accession);
            my $study_accession;
            my $samp_count = {};
            while(my $row = $sth_ena->fetchrow_hashref()){
                if (defined $row->{'SUBMISSION_XML'}){
                    my $parser = XML::LibXML->new();
                    my $doc;
                    eval{
                        $doc = $parser->parse_string($row->{'SUBMISSION_XML'});
                    };
                    if (!$doc){
                        print "Failed to parse SUBMISSION_XML, cannot determine DESCRIPTION!";
                    }
                    my $root = $doc->getDocumentElement;
                    my $nodes = $root->find('/SUBMISSION_SET/SUBMISSION/ACTIONS/ACTION');
                    foreach my $node(@$nodes){
                        my @child_nodes = $node->nonBlankChildNodes();
                        foreach my $child(@child_nodes){
                            $row->{'ACTION'} = $child->nodeName;
                        }

                    }
                    $nodes = $root->find('/SUBMISSION_SET/SUBMISSION');
                    foreach my $node(@$nodes){
                        $row->{'TITLE'} = $node->getAttribute('alias');
                    }
                    delete $row->{'SUBMISSION_XML'};
                }
                if ($row->{'UPDATED'} =~ m/\s+(.*$)/){
                    $row->{'UPDATED'} =~ s/\s+(.*$)//;
                    $row->{'UPDATED'} = _oracle_to_mysql_date($row->{'UPDATED'});
                    $row->{'UPDATED'} .= ' '.$1;
                }
                $ena_submission_id = $row->{'SUBMISSION_ID'};
                $sth_eva->execute($row->{'SUBMISSION_ID'},$row->{'ACTION'},$row->{'TITLE'},$row->{'UPDATED'},1,'PROJECT');
                my $id = $sth_eva->fetchrow_hashref();
                push(@{$accession_to_id->{'SUBMISSION_ID'}},$id->{'submission_id'});
            }
            $sth_eva = $dbh_eva->prepare("insert into PROJECT_ENA_SUBMISSION (PROJECT_ACCESSION,SUBMISSION_ID) values (?,?)");
            foreach my $sub_id(@{$accession_to_id->{'SUBMISSION_ID'}}){
                $sth_eva->execute($project_accession,$sub_id);
            }
            $sth_eva = $dbh_eva->prepare("insert into EVA_SUBMISSION (EVA_SUBMISSION_ID,EVA_SUBMISSION_STATUS_ID) values (?,?)");
            $sth_eva->execute($eload,6);
            $sth_eva = $dbh_eva->prepare("insert into PROJECT_EVA_SUBMISSION (PROJECT_ACCESSION,old_ticket_id,ELOAD_ID) values (?,?,?)");
            $sth_eva->execute($project_accession,$eload,$eload);
        } else {
            my $rows = _read_from_project_xml($project_file,$dbh_eva,$submission_file);
            $project_accession=$rows->[0]->{'PROJECT_ID'};
        }

    }
    if ($project_only == 0){
        warn "Loading Analysis data for $analysis_accession";
        ### Insert into ANALYSIS table
        my $analysis_in_ena = 0;
        if (!defined $analysis_accession){
        $sth_ena = $dbh_ena->prepare(<<sqlend);
select
    count(*) from era.analysis
where
    (
      status_id <> 5 and
      lower(SUBMISSION_ACCOUNT_ID) in ($sql_placeholder) and
      (
        study_id in (select study_id from era.study where project_id=?) or
        study_id=? or
        bioproject_id=?
      )
    )
sqlend
        $sth_ena->execute(@user_accounts, $project_accession,$project_accession,$project_accession);
        while(my @row = $sth_ena->fetchrow_array()){
            $analysis_in_ena = $row[0];
        }
        } else {
        $sth_ena = $dbh_ena->prepare(<<sqlend);
select
  count(*)
from
  era.analysis
where
  (
    status_id <> 5
    and lower(SUBMISSION_ACCOUNT_ID) in ($sql_placeholder)
    and (
      study_id in (select study_id from era.study where project_id=?)
      or study_id=?
      or bioproject_id=?
    )
    and analysis_id=?
  )
sqlend
	$sth_ena->execute(@user_accounts, $project_accession, $project_accession, $project_accession, $analysis_accession);
	while(my @row = $sth_ena->fetchrow_array()){
            $analysis_in_ena = $row[0];
        }
        }

        my $read = 0;
        my @project_analysis;
        if ($analysis_in_ena){
            warn "Analysis is in ENA";
            my $ena_sql = 'select
  t.analysis_id,
  t.analysis_title,
  t.analysis_alias,
  t.analysis_type,
  t.center_name,
  t.first_created,
  xmltype.getclobval(t.ANALYSIS_XML) analysis_xml,
  x.assembly assembly,
  x.refname refname,
  y.custom
from
  era.analysis t
  left outer join XMLTABLE(\'/ANALYSIS_SET//ANALYSIS_TYPE//SEQUENCE_VARIATION//ASSEMBLY//STANDARD\' passing t.analysis_xml columns assembly varchar2(2000) path \'@accession\', refname varchar2(2000) path \'@refname\') x on (1=1)
  left outer join XMLTABLE(\'/ANALYSIS_SET//ANALYSIS_TYPE//SEQUENCE_VARIATION//ASSEMBLY//CUSTOM//URL_LINK\' passing t.analysis_xml columns custom varchar2(2000) path \'URL\') y on (1=1)
where t.status_id <> 5 and '."
  lower(SUBMISSION_ACCOUNT_ID) in ($sql_placeholder)
  and (
       study_id in (select study_id from era.study where project_id=?)
       or study_id=?
       or bioproject_id=?
      )";

            $sth_ena = $dbh_ena->prepare($ena_sql);

            $sth_ena->execute(@user_accounts,$project_accession,$project_accession,$project_accession);
            my $sample_hash = {};
            my $sample_count = 0;
            my $analysis_id = -1;
            while (my $row = $sth_ena->fetchrow_hashref()){
                if ($row->{'ANALYSIS_TYPE'} ne "SEQUENCE_VARIATION") {
                    next;
                }
                warn $row->{'ANALYSIS_ID'};
                $analysis_id = $row->{'ANALYSIS_ID'};
                if (defined $row->{'ANALYSIS_XML'}){
                    if (defined $analysis_accession){
                        next if ($row->{'ANALYSIS_ID'} ne $analysis_accession);
                    }
                    my @platforms;
                    warn "Got data from ENA";
                    my $parser = XML::LibXML->new();
                    my $doc;
                    my $browsable_assembly_set_id;
                    eval{
                        $doc = $parser->parse_string($row->{'ANALYSIS_XML'});
                    };
                    if (!$doc){
                        print "Failed to parse ANALYSIS_XML, cannot determine DESCRIPTION!";
                    }
                    my $root = $doc->getDocumentElement();
                    my $desc_nodes = $root->find('/ANALYSIS_SET/ANALYSIS/DESCRIPTION');
                    $row->{'DESCRIPTION'} = $desc_nodes->get_node(0)->textContent;
                    $row->{'DESCRIPTION'} =~ s/<\/*p>//ig;
                    my @plat_nodes = $root->findnodes('/ANALYSIS_SET/ANALYSIS/ANALYSIS_TYPE/SEQUENCE_VARIATION/PLATFORM');
                    foreach my $plat(@plat_nodes){
                        push(@platforms,$plat->textContent());
                    }
                    delete $row->{'ANALYSIS_XML'};

                    if (defined $row->{'ASSEMBLY'}){
                        $sth_eva = $dbh_eva->prepare("insert into ANALYSIS (ANALYSIS_ACCESSION,CENTER_NAME,ALIAS,TITLE,DESCRIPTION,DATE,vcf_reference,vcf_reference_accession) values (?,?,?,?,?,?,?,?)");
                        $sth_eva->execute($row->{'ANALYSIS_ID'},$row->{'CENTER_NAME'},$row->{'ANALYSIS_ALIAS'},$row->{'ANALYSIS_TITLE'},$row->{'DESCRIPTION'},$row->{'FIRST_CREATED'},$row->{'REFNAME'},$row->{'ASSEMBLY'});
                        $sth_eva = $dbh_eva->prepare("select assembly_set_id from accessioned_assembly where assembly_accession = ?");
                        my $asssetid = -1;
                        $sth_eva->execute($row->{'ASSEMBLY'});
                        while(my @rowz = $sth_eva->fetchrow_array()){
                            $asssetid = $rowz[0];
                        }
                        if ($asssetid == -1){
                            warn "No assembly defined for ".$row->{'ASSEMBLY'};
                            my $sth_eta = $dbh_eta->prepare("select SET_ACC,SET_CHAIN,SET_VERSION,TAX_ID,NAME from ETA.GC_ASSEMBLY_SET where SET_ACC=?");
                            my $sth_eva_assem1 = $dbh_eva->prepare("insert into evapro.assembly_set(taxonomy_id,assembly_name) values (?,?)");
                            $sth_eta->execute($row->{'ASSEMBLY'});
                            while(my @row_eta = $sth_eta->fetchrow_array()){
                                $sth_eva_assem1->execute($row_eta[3],$row_eta[4]);
                                $sth_eva_assem1 = $dbh_eva->prepare("select assembly_set_id from evapro.assembly_set where taxonomy_id=? and assembly_name=?");
                                my $sth_eva_assem2 = $dbh_eva->prepare("insert into evapro.accessioned_assembly(assembly_set_id,assembly_accession,assembly_chain,assembly_version) values (?,?,?,?)");
                                $sth_eva_assem1->execute($row_eta[3],$row_eta[4]);
                                my $assem_set_id = 0;
                                while (my @row_eva_assem1 = $sth_eva_assem1->fetchrow_array()){
                                    $assem_set_id = $row_eva_assem1[0];
                                }
                                $sth_eva_assem2->execute($assem_set_id,$row_eta[0],$row_eta[1],$row_eta[2]);
                            }
                            $sth_eva = $dbh_eva->prepare("select assembly_set_id from accessioned_assembly where assembly_accession = ?");
                            $sth_eva->execute($row->{'ASSEMBLY'});
                            while(my @rowz = $sth_eva->fetchrow_array()){
                                $asssetid = $rowz[0];
                            }
                            if ($asssetid == -1){
                                warn "Add to tables assembly_set and either accessioned_assembly or custom_assembly";
                                exit(1);
                            }

                        }
                        $browsable_assembly_set_id = $asssetid;
                    } else {
                        $sth_eva = $dbh_eva->prepare("select assembly_set_id from custom_assembly where assembly_location=? and assembly_file_name=?");
                        my @tmp_asemb = split(/\//,$row->{'CUSTOM'});
                        my $fi = pop(@tmp_asemb);
                        $sth_eva->execute(join("/",@tmp_asemb),$fi);
                        my $assset_id = -1;
                        while (my @rowz = $sth_eva->fetchrow_array()){
                            $assset_id = $rowz[0];
                        }
                        if ($assset_id == -1){
                            $sth_eva = $dbh_eva->prepare("select taxonomy_id from project_taxonomy where project_accession=?");
                            $sth_eva->execute($project_accession);
                            my $tmp_tax = -1;
                            while (my @rowz = $sth_eva->fetchrow_array()){
                                $tmp_tax = $rowz[0];
                            }
                            if ($tmp_tax == -1){
                                warn "Unable to determine taxonomy for project!";
                                exit(1);
                            }
                            $sth_eva = $dbh_eva->prepare("select assembly_set_id from assembly_set where taxonomy_id=? and assembly_name=?");
                            $sth_eva->execute($tmp_tax,$fi);
                            while (my @rowz = $sth_eva->fetchrow_array()){
                                $assset_id = $rowz[0];
                            }
                            if ($assset_id == -1){
                                $sth_eva = $dbh_eva->prepare("insert into assembly_set (taxonomy_id,assembly_name) values (?,?)");
                                $sth_eva->execute($tmp_tax,$fi);
                                $sth_eva = $dbh_eva->prepare("select assembly_set_id from assembly_set where taxonomy_id=? and assembly_name=?");
                                $sth_eva->execute($tmp_tax,$fi);
                                while(my @rowz = $sth_eva->fetchrow_array()){
                                    $assset_id = $rowz[0];
                                }
                                $sth_eva = $dbh_eva->prepare("insert into custom_assembly (assembly_set_id,assembly_location,assembly_file_name) values (?,?,?)");
                                $sth_eva->execute($assset_id,join("/",@tmp_asemb),$fi);
                            } else {
                                warn "custom assembly appears to be used already, but location different";
                                warn $fi;
                                exit(1);
                            }

                        }


                        $sth_eva = $dbh_eva->prepare("insert into ANALYSIS (ANALYSIS_ACCESSION,CENTER_NAME,ALIAS,TITLE,DESCRIPTION,DATE,assembly_set_id) values (?,?,?,?,?,?,?)");
                        $sth_eva->execute($row->{'ANALYSIS_ID'},$row->{'CENTER_NAME'},$row->{'ANALYSIS_ALIAS'},$row->{'ANALYSIS_TITLE'},$row->{'DESCRIPTION'},$row->{'FIRST_CREATED'},$assset_id);
                        $browsable_assembly_set_id = $assset_id;
                    }

                    my $sth_ena2 = $dbh_ena->prepare("select sample_id from era.analysis_sample where analysis_id=?");
                    $sth_ena2->execute($row->{'ANALYSIS_ID'});
                    while(my @row2 = $sth_ena2->fetchrow_array()){
                        $sample_hash->{$row2[0]}++;
                    }
                    $sth_eva = $dbh_eva->prepare("select platform_id,platform,manufacturer from platform");
                    $sth_eva->execute();
                    my $plats = {};
                    while(my $row = $sth_eva->fetchrow_hashref()){
                        $plats->{$row->{'platform'}}->{'platform'} = $row->{'platform_id'};
                    }
                    $sth_eva = $dbh_eva->prepare("insert into analysis_platform(analysis_accession,platform_id) values (?,?)");
                    foreach my $plat(@platforms){
                        my @pls = split(/,/,$plat);
                        foreach my $pl(@pls){

                            $pl =~ s/^\s+|\s+$//g;
                            $pl =~ s/GAII/Genome Analyzer II/;
                            $pl =~ s/Illumina Genome Analyzer Iix/Illumina Genome Analyzer IIx/;
                            if (defined $plats->{$pl}){

                                $sth_eva->execute($row->{'ANALYSIS_ID'},$plats->{$pl}->{'platform'});
                            } else {
                                warn "No platforms inserted. Is the ENA platform |".$plat."| in the EVA platform CV?";
                            }
                        }
                    }
                    $sth_eva = $dbh_eva->prepare("insert into ANALYSIS_SUBMISSION (ANALYSIS_ACCESSION,SUBMISSION_ID) values (?,?)");
                    foreach my $sub_id(@{$accession_to_id->{'SUBMISSION_ID'}}){
                        $sth_eva->execute($row->{'ANALYSIS_ID'},$sub_id);
                    }
                    $sth_eva = $dbh_eva->prepare("insert into PROJECT_ANALYSIS (PROJECT_ACCESSION,ANALYSIS_ACCESSION) values (?,?)");
                    push(@project_analysis,$row->{'ANALYSIS_ID'});
                    $sth_eva->execute($project_accession,$row->{'ANALYSIS_ID'});

            ### Insert into FILE

        ## Get sample counts

                    my $sql = 'select distinct asf.analysis_id as analysis_accession, wf.' . $sub_file_id_col . ' as submission_file_id,regexp_substr(wf.data_file_path, \'[^/]*$\') as filename,wf.checksum as file_md5,wf.data_file_format as file_type, ana.status_id from era.analysis_submission_file asf
       join era.webin_file wf on asf.analysis_id=wf.data_file_owner_id join era.analysis ana on asf.analysis_id=ana.analysis_id where '."asf.analysis_id=?";


                    my $sth2_ena = $dbh_ena->prepare($sql);

                        $sth2_ena->execute($row->{'ANALYSIS_ID'});
                        while (my $row2 = $sth2_ena->fetchrow_hashref()){
                            my @fs = split(/\//,$row2->{'FILENAME'});
                            $row2->{'FILENAME'} = $fs[-1];
                            $sth_eva = $dbh_eva->prepare("select file_id from file where file_md5 = ? and filename = ?");
                            $sth_eva->execute($row2->{'FILE_MD5'}, $row2->{'FILENAME'});
                            my $file_id = -1;
                            while(my @rowz = $sth_eva->fetchrow_array()){
                                if ($rowz[0] =~ /^\d+$/){
                                    $file_id = $rowz[0];
                                }
                            }
                            if (!defined $browsable_assembly_set_id){
                                exit(1);
                            }
                            if ($file_id < 0){
                                $sth_eva = $dbh_eva->prepare("insert into FILE (FILENAME,FILE_MD5,FILE_TYPE,ENA_SUBMISSION_FILE_ID,FILE_CLASS,FILE_VERSION,IS_CURRENT,FILE_LOCATION,FTP_FILE) values (?,?,?,?,?,?,?,?,?)");
                                my $ftp_file_path = get_ftp_file_path($ena_ftp_file_prefix_path, $file_class, $row2->{'ANALYSIS_ACCESSION'}, $row2->{'FILENAME'});
                                $sth_eva->execute($row2->{'FILENAME'},$row2->{'FILE_MD5'},$row2->{'FILE_TYPE'},$row2->{'SUBMISSION_FILE_ID'},$file_class,$file_version,1,$file_location,$ftp_file_path);
                                my $sth2_eva = $dbh_eva->prepare("select file_id from file where file_md5=? and filename=?");
                                $sth2_eva->execute($row2->{'FILE_MD5'}, $row2->{'FILENAME'});
                                while(my @rowz = $sth2_eva->fetchrow_array()){
                                    $file_id = $rowz[0] if ($file_id < 0);
                                }
                                if ((($row2->{'FILE_TYPE'} =~ /^vcf$/i)||($row2->{'FILE_TYPE'} =~ /^vcf_aggregate$/i)) && ($row2->{'STATUS_ID'} == 4)){
                                    $sth_eva = $dbh_eva->prepare("insert into browsable_file (file_id,ena_submission_file_id,filename,project_accession,assembly_set_id) values (?,?,?,?,?)");
                                    $sth_eva->execute($file_id,$row2->{'SUBMISSION_FILE_ID'},$row2->{'FILENAME'},$project_accession,$browsable_assembly_set_id);
                                }
                            }
                            $sth_eva = $dbh_eva->prepare("insert into ANALYSIS_FILE (ANALYSIS_ACCESSION,FILE_ID) values (?,?)");
                            $sth_eva->execute($row2->{'ANALYSIS_ACCESSION'},$file_id);
                        }
#                   }



                    ### Insert into EVA_REFERENCED_SEQUENCE and ANALYSIS_SEQUENCE



                    $sql = 'select t.analysis_id analysis_accession,x.accession sequence_accession,x.label sequence_label,x.refname sequence_refname
                    from
                    era.analysis t, xmltable(\'/ANALYSIS_SET//SEQUENCE\' passing t.analysis_xml columns accession varchar2(2000) path \'@accession\', label varchar2(1000) path \'@label\', refname varchar2(1000) path \'@refname\' ) x '."
                    where analysis_id=?";
                    $sth2_ena = $dbh_ena->prepare($sql);
#                   foreach my $anaacc(@project_analysis){
                        $sth2_ena->execute($row->{'ANALYSIS_ID'});
                        while (my $row2 = $sth2_ena->fetchrow_hashref()){
                            my $custom = 0;
                            my $sth_eva3 = $dbh_eva->prepare("select sequence_id from EVA_REFERENCED_SEQUENCE where sequence_accession = ?");
                            $sth_eva3->execute($row2->{'SEQUENCE_ACCESSION'});
                            my $id = -1;
                            while(my @rowz = $sth_eva3->fetchrow_array){
                                $id = $rowz[0];
                            }
                            if ($id == -1){
                                if ((defined $row2->{'SEQUENCE_ACCESSION'})&&($row2->{'SEQUENCE_ACCESSION'} =~ /\w+/)){
                                    $sth_eva3 = $dbh_eva->prepare("insert into EVA_REFERENCED_SEQUENCE (SEQUENCE_ACCESSION,LABEL,REF_NAME) values (?,?,?)");
                                    $sth_eva3->execute($row2->{'SEQUENCE_ACCESSION'},$row2->{'SEQUENCE_LABEL'},$row2->{'SEQUENCE_REFNAME'});

                                    my $sth4_eva = $dbh_eva->prepare("select currval('eva_referenced_sequence_sequence_id_seq')");
                                    $sth4_eva->execute();
                                    while(my @rowz = $sth4_eva->fetchrow_array()){
                                        $id = $rowz[0];
                                    }
                                } else {
                                    $custom = 1;
                                }
                            }
                            if ($custom == 0){
                                $sth_eva3 = $dbh_eva->prepare("insert into ANALYSIS_SEQUENCE (ANALYSIS_ACCESSION,SEQUENCE_ID) values (?,?)");
                                $sth_eva3->execute($row2->{'ANALYSIS_ACCESSION'},$id);
                            }
                        }
#                   }
                    ### Insert into
                    $sql = qq|select analysis_id,extractValue(value(x),'/EXPERIMENT_TYPE') as experiment_type from era.ANALYSIS,table(xmlsequence(extract(analysis.analysis_xml,'/ANALYSIS_SET//EXPERIMENT_TYPE'))) x where analysis.analysis_id=?|;
                    $sth2_ena = $dbh_ena->prepare($sql);
                    foreach my $anaacc(@project_analysis){
                        $sth2_ena->execute($anaacc);
                        while (my $row2 = $sth2_ena->fetchrow_hashref()){
                            _insert_experiment($dbh_eva, $anaacc, $row2->{'EXPERIMENT_TYPE'});
                        }
                    }
                } else {
                    warn "$row->{'ANALYSIS_XML'} is undefined";
                    exit(1);
                }
            }
            if ($analysis_id eq -1){
                warn "Could not parse analysis from ENA";
                exit(1);
            }
            $sample_count = scalar(keys %$sample_hash);
            $sth_eva = $dbh_eva->prepare("insert into project_samples_temp1 (project_accession,sample_count) values (?,?)");
            $sth_eva->execute($project_accession,$sample_count);
        } else {
            warn "Loading from analysis file";
            if (defined $analysis_file){
                if ($read == 0){
                    _read_from_analysis_xml($analysis_file,\@project_analysis,$dbh_eva);
                }
            }
            $read = 1;
        }
    }

    $dbh_eva->commit();
};
if ($@){
    warn "Transaction aborted! $@";
    $dbh_eva->rollback();
}


$dbh_eva->disconnect();
$dbh_ena->disconnect();

sub _oracle_to_mysql_date{
    my $ora_date = shift;
    my @a = split(/-/,$ora_date);
    if ($a[1] =~ /jan/i){
        $a[1] = '01';
    } elsif ($a[1] =~ /feb/i){
        $a[1] = '02';
    } elsif ($a[1] =~ /mar/i){
        $a[1] = '03';
    } elsif ($a[1] =~ /apr/i){
        $a[1] = '04';
    } elsif ($a[1] =~ /may/i){
        $a[1] = '05';
    } elsif ($a[1] =~ /jun/i){
        $a[1] = '06';
    } elsif ($a[1] =~ /jul/i){
        $a[1] = '07';
    } elsif ($a[1] =~ /aug/i){
        $a[1] = '08';
    } elsif ($a[1] =~ /sep/i){
        $a[1] = '09';
    } elsif ($a[1] =~ /oct/i){
        $a[1] = '10';
    } elsif ($a[1] =~ /nov/i){
        $a[1] = '11';
    } elsif ($a[1] =~ /dec/i){
        $a[1] = '12';
    }
    my $t = $a[0];
    $a[0] = $a[2];
    $a[2] = $t;
    return join ('-',@a);
}

sub _read_from_project_xml{
    my $xml = shift;
    my $dbh = shift;
    my $sub_xml = shift;
    my $parser = XML::LibXML->new();
    my $doc;
    warn "Reading from Project XML";
    eval{
        $doc = $parser->parse_file($xml);
    };
    if (!$doc){
        print "Failed to parse PROJECT_XML";
    }
    my @project_sets = $doc->getElementsByTagName('PROJECT_SET');
    my $max = 0;
    my $count = 1;
    my @rows;
    foreach my $project_set(@project_sets){
        my $row;
        my @projects = $project_set->getChildrenByTagName('PROJECT');
        my @publicat;
        foreach my $project(@projects){
            my @pubss = $project->getChildrenByTagName('PUBLICATIONS');
            foreach my $pubs(@pubss){
                my @pub = $pubs->getChildrenByTagName('PUBLICATION');
                foreach my $p(@pub){
                    my @pls = $p->getChildrenByTagName('PUBLICATION_LINKS');
                    foreach my $pl(@pls){
                        my @pzs = $pl->getChildrenByTagName('PUBLICATION_LINK');
                        foreach my $pz(@pzs){
                            my @xls = $pz->getChildrenByTagName('XREF_LINK');
                            foreach my $xl(@xls){
                                my @dbs = $xl->getChildrenByTagName('DB');
                                my @ids = $xl->getChildrenByTagName('ID');
                                for (my $i=0;$i<=$#dbs;$i++){
                                    my $zz->{'db'} = $dbs[$i]->textContent();
                                    $zz->{'id'} = $ids[$i]->textContent();
                                    push(@publicat,$zz);
                                }
                            }
                        }

                    }
                }
            }
            $row->{'PROJECT_ALIAS'} = $project->getAttribute('alias');
            $row->{'CENTER_NAME'} = $project->getAttribute('center_name');
            $row->{'PROJECT_ID'} = $project->getAttribute('accession');
            if (!defined $row->{'PROJECT_ID'}){
                if ($max == 0){
                    my $sth = $dbh->prepare("select project_accession from PROJECT where project_accession like \'PRJX%\' order by(project_accession)");
                    $sth->execute();
                    while (my @res = $sth->fetchrow_array()){
                        $res[0] =~ s/PRJX//;
                        $res[0] = $res[0] * 1;
                        if ($res[0] > $max){
                            $max = $res[0] * 1;
                        }
                    }
                }
                warn "Creating dummy project accession";
                my $acc_num = $max + $count;
                $count++;
                if ($acc_num < 10){
                    $row->{'PROJECT_ID'} = 'PRJX0000'.$acc_num;
                } elsif ($acc_num < 100){
                    $row->{'PROJECT_ID'} = 'PRJX000'.$acc_num;
                } elsif ($acc_num <1000){
                    $row->{'PROJECT_ID'} = 'PRJX00'.$acc_num;
                } elsif ($acc_num <10000){
                    $row->{'PROJECT_ID'} = 'PRJX0'.$acc_num;
                } elsif ($acc_num <100000){
                    $row->{'PROJECT_ID'} = 'PRJX'.$acc_num;
                } else {
                    warn "Run out of PRJX accessions!";
                }
                warn $row->{'PROJECT_ID'};
            } else {
                warn "Reading defined analysis accession - ". $row->{'PROJECT_ID'};
            }
            my @titles = $project->getChildrenByTagName('TITLE');
            foreach my $title(@titles){
                $row->{'PROJECT_TITLE'} = $title->textContent();
            }
            my @descs = $project->getChildrenByTagName('DESCRIPTION');
            foreach my $desc(@descs){

                $row->{'DESCRIPTION'} = $desc->textContent();
            }
            my @sub_projs = $project->getChildrenByTagName('SUBMISSION_PROJECT');
            foreach my $sub_proj(@sub_projs){
                $row->{'SCOPE'} = $sub_proj->getAttribute('scope');
                $row->{'MATERIAL'} = $sub_proj->getAttribute('material');
                my @seq_projs = $sub_proj->getChildrenByTagName('SEQUENCING_PROJECT');
                foreach my $seq_proj(@seq_projs){

                }
                my @organisms = $sub_proj->getChildrenByTagName('ORGANISM');
                foreach my $organism(@organisms){
                    my @taxons = $organism->getChildrenByTagName('TAXON_ID');
                    foreach my $taxon(@taxons){
                        $row->{'TAX_ID'} = $taxon->textContent();
                    }
                }
            }
        }
        push(@rows,$row);
        $sth_eva = $dbh_eva->prepare("insert into PROJECT (PROJECT_ACCESSION,CENTER_NAME,ALIAS,TITLE,DESCRIPTION,SCOPE,MATERIAL,SELECTION,SECONDARY_STUDY_ID) values (?,?,?,?,?,?,?,?,?)");
        $sth_eva->execute($row->{'PROJECT_ID'},$row->{'CENTER_NAME'},$row->{'PROJECT_ALIAS'},$row->{'PROJECT_TITLE'},$row->{'DESCRIPTION'},$row->{'SCOPE'},$row->{'MATERIAL'},'other',$row->{'PROJECT_ID'});
        warn $row->{'PROJECT_ID'};
        $sth_eva = $dbh_eva->prepare("select project_accession_code from project");
        $sth_eva->execute();
        while(my @row1 = $sth_eva->fetchrow_array()){
            $accession_to_id->{'PROJECT_ID'} = $row1[0];
            my $e = $row1[0] + 1;
            my $sth2_eva = $dbh_eva->prepare("update project set eva_study_accession=".$e." where project_accession = ?");
            $sth2_eva->execute($row->{'PROJECT_ID'});
        }
        foreach my $pp(@publicat){
            $sth_eva = $dbh_eva->prepare("select dbxref_id from dbxref where db=? and id=? and source_object=?");
            $sth_eva->execute($pp->{'db'},$pp->{'id'},'project');
            my $dbxref = {};
            while(my @row1 = $sth_eva->fetchrow_array()){
                $dbxref->{'dbxref_id'} = $row1[0];
            }
            if (!defined $dbxref->{'dbxref_id'}){
                $sth_eva = $dbh_eva->prepare("insert into dbxref (db,id,link_type,source_object) values (?,?,?,?) returning dbxref_id");
                $sth_eva->execute($pp->{'db'},$pp->{'id'},'publication','project');
                $dbxref = $sth_eva->fetchrow_hashref();
            }
            my $sth2_eva = $dbh_eva->prepare("insert into project_dbxref(project_accession,dbxref_id) values (?,?)");
            $sth2_eva->execute($project_accession,$dbxref->{'dbxref_id'});
        }

        $sth_eva = $dbh_eva->prepare("select taxonomy_id from TAXONOMY where TAXONOMY_ID=?");
        $sth_eva->execute($row->{'TAX_ID'});
        my $tax_id_in_eva = 0;
        while (my $row1 = $sth_eva->fetchrow_hashref()){
            if ($row1->{'taxonomy_id'} == $row->{'TAX_ID'}){
                $tax_id_in_eva++;
                my $sth_eva2 = $dbh_eva->prepare("insert into PROJECT_TAXONOMY (PROJECT_ACCESSION,TAXONOMY_ID) values (?,?)");
                $sth_eva2->execute($row->{'PROJECT_ID'},$row->{'TAX_ID'});
            } else {
                my $sth_eva = $dbh_eva->prepare("insert into TAXONOMY(TAXONOMY_ID,COMMON_NAME,SCIENTIFIC_NAME) values(?,?,?)");
                $sth_eva->execute($row->{'TAX_ID'},$row->{'COMMON_NAME'},$row->{'SCIENTIFIC_NAME'});
                my $sth_eva2 = $dbh_eva->prepare("insert into PROJECT_TAXONOMY (PROJECT_ACCESSION,TAXOMONY_ID) values (?,?)");
                $sth_eva2->execute($row->{'PROJECT_ID'},$row->{'TAX_ID'});
            }
        }
        if (!$tax_id_in_eva){
            my $sth_eva = $dbh_eva->prepare("insert into TAXONOMY(TAXONOMY_ID,COMMON_NAME,SCIENTIFIC_NAME) values(?,?,?)");
            $sth_eva->execute($row->{'TAX_ID'},$row->{'COMMON_NAME'},$row->{'SCIENTIFIC_NAME'});
            my $sth_eva2 = $dbh_eva->prepare("insert into PROJECT_TAXONOMY (PROJECT_ACCESSION,TAXONOMY_ID) values (?,?)");
            $sth_eva2->execute($row->{'PROJECT_ID'},$row->{'TAX_ID'});
        }
        _read_from_submission_xml($sub_xml,$dbh,$row->{'PROJECT_ID'});

    }

    return (\@rows);
}

sub _read_from_analysis_xml{
    my $xml = shift;
    my $project_analysis = shift;
    my $dbh = shift;
    my $parser = XML::LibXML->new();
    my $doc;
    warn "Reading from $xml";
    eval{
        $doc = $parser->parse_file($xml);
    };
    if (!$doc){
        print "Failed to parse ANALYSIS_XML, cannot determine DESCRIPTION!";
    }
    my @analysis_sets = $doc->getElementsByTagName('ANALYSIS_SET');
    my $max = 0;
    my $count = 1;
    foreach my $analysis_set(@analysis_sets){

        my @analyses = $analysis_set->getChildrenByTagName('ANALYSIS');
        ANALYSES_LOOP:
        foreach my $analysis(@analyses){
            my @analysis_types = $analysis->getChildrenByTagName('ANALYSIS_TYPE');
            foreach my $analysis_type(@analysis_types){
                if (defined $analysis_type->getChildrenByTagName('REFERENCE_ALIGNMENT')) {
                    next ANALYSES_LOOP;
                }
            }
            my $row;
            $row->{'ANALYSIS_ALIAS'} = $analysis->getAttribute('alias');
            $row->{'ANALYSIS_ID'} = $analysis->getAttribute('accession');
            if (!defined $row->{'ANALYSIS_ID'}){
                if ($max == 0){
                    my $sth = $dbh->prepare("select analysis_accession from ANALYSIS where analysis_accession like \'ERZX%\' order by(analysis_accession)");
                    $sth->execute();
                    while (my @res = $sth->fetchrow_array()){
                        $res[0] =~ s/ERZX//;
                        $res[0] = $res[0] * 1;
                        if ($res[0] > $max){
                            $max = $res[0] * 1;
                        }
                    }
                }
                warn "Creating dummy analysis accession";
                my $acc_num = $max + $count;
                $count++;
                if ($acc_num < 10){
                    $row->{'ANALYSIS_ID'} = 'ERZX0000'.$acc_num;
                } elsif ($acc_num < 100){
                    $row->{'ANALYSIS_ID'} = 'ERZX000'.$acc_num;
                } elsif ($acc_num <1000){
                    $row->{'ANALYSIS_ID'} = 'ERZX00'.$acc_num;
                } elsif ($acc_num <10000){
                    $row->{'ANALYSIS_ID'} = 'ERZX0'.$acc_num;
                } elsif ($acc_num <100000){
                    $row->{'ANALYSIS_ID'} = 'ERZX'.$acc_num;
                } else {
                    warn "Run out of ERZX accessions!";
                }
            } else {
                warn "Reading defined analysis accession - ". $row->{'ANALYSIS_ID'};
            }
            $row->{'CENTER_NAME'} = $analysis->getAttribute('center_name');
            my @titles = $analysis->getChildrenByTagName('TITLE');
            foreach my $title(@titles){
                $row->{'ANALYSIS_TITLE'} = $title->textContent();
            }
            my @descs = $analysis->getChildrenByTagName('DESCRIPTION');
            foreach my $desc(@descs){

                $row->{'DESCRIPTION'} = $desc->textContent();
            }
            my @studies = $analysis->getChildrenByTagName('STUDY_REF');
            foreach my $study(@studies){

                $row->{'study'} = $study->getAttribute('accession');
            }
            my @samps = $analysis->getChildrenByTagName('SAMPLE_REF');
            foreach my $samp(@samps){
                push(@{$row->{'samples'}},$samp->getAttribute('accession'));
            }
            foreach my $analysis_type(@analysis_types){
                my @seq_vars = $analysis_type->getChildrenByTagName('SEQUENCE_VARIATION');
                foreach my $seq_var(@seq_vars){
                    my @assemblies = $seq_var->getChildrenByTagName('ASSEMBLY');
                    foreach my $assembly(@assemblies){
                        my @stands = $assembly->getChildrenByTagName('STANDARD');
                        foreach my $stand(@stands){
                            $row->{'assembly'} = $stand->getAttribute('accession');
                        }
                    }
                    my @seqs = $seq_var->getChildrenByTagName('SEQUENCE');
                    foreach my $seq(@seqs){
                        my $seq_hash->{'accession'} = $seq->getAttribute('accession');
                        $seq_hash->{'label'} = $seq->getAttribute('label');
                        push(@{$row->{'sequences'}},$seq_hash);
                    }
                    my @exps = $seq_var->getChildrenByTagName('EXPERIMENT_TYPE');
                    foreach my $exp(@exps){
                        push(@{$row->{'experiment'}},$exp->textContent());
                    }
                }
            }
            my @fileses = $analysis->getChildrenByTagName('FILES');
            foreach my $filess(@fileses){
                my @files = $filess->getChildrenByTagName('FILE');
                foreach my $file(@files){
                    my $file_hash = {};
                    $file_hash->{'filename'} = $file->getAttribute('filename');
                    $file_hash->{'filetype'} = $file->getAttribute('filetype');
                    $file_hash->{$file->getAttribute('checksum_method')} = $file->getAttribute('checksum');
                    push(@{$row->{'files'}},$file_hash);
                }
            }

#           Inserting ANALYSIS
            $sth_eva = $dbh_eva->prepare("select assembly_name from accessioned_assembly join assembly_set using(assembly_set_id) where assembly_accession=?");
            if ((defined $row->{'assembly'})&&($row->{'assembly'} =~ /\w+/)){
                $sth_eva->execute($row->{'assembly'});
                while (my @r1 = $sth_eva->fetchrow_array()){
                    $row->{'assembly_name'} = $r1[0];
                }
            } else {
                warn "No assembly accession given!";
                exit(1);
            }
            $sth_eva = $dbh_eva->prepare("insert into ANALYSIS (ANALYSIS_ACCESSION,CENTER_NAME,ALIAS,TITLE,DESCRIPTION,DATE,vcf_reference_accession,vcf_reference) values (?,?,?,?,?,?,?,?)");
            $sth_eva->execute($row->{'ANALYSIS_ID'},$row->{'CENTER_NAME'},$row->{'ANALYSIS_ALIAS'},$row->{'ANALYSIS_TITLE'},$row->{'DESCRIPTION'},$row->{'FIRST_CREATED'},$row->{'assembly'},$row->{'assembly_name'});
            $accession_to_id->{'ANALYSIS_ID'} = $sth_eva->{mysql_insertid};
            $sth_eva = $dbh_eva->prepare("insert into ANALYSIS_SUBMISSION (ANALYSIS_ACCESSION,SUBMISSION_ID) values (?,?)");
            foreach my $sub_id(@{$accession_to_id->{'SUBMISSION_ID'}}){
                $sth_eva->execute($row->{'ANALYSIS_ID'},$sub_id);
            }
            $sth_eva = $dbh_eva->prepare("insert into PROJECT_ANALYSIS (PROJECT_ACCESSION,ANALYSIS_ACCESSION) values (?,?)");
            push(@$project_analysis,$row->{'ANALYSIS_ID'});
            $sth_eva->execute($project_accession,$row->{'ANALYSIS_ID'});
            my $file_count = 0;
            my $assembly_set;
            $sth_eva = $dbh_eva->prepare("select assembly_set_id from accessioned_assembly where assembly_accession=?");
            $sth_eva->execute($row->{'assembly'});
            while(my @r1 = $sth_eva->fetchrow_array()){
                $assembly_set = $r1[0];
            }
            my $assembly_code;
            $sth_eva = $dbh_eva->prepare("select assembly_code from assembly_set where assembly_set_id=?");
            $sth_eva->execute($assembly_set);
            while(my @r1 = $sth_eva->fetchrow_array()){
                $assembly_code = $r1[0];
            }
            if (!defined $assembly_set){
                warn "Unknown assembly " . $row->{'assembly'};
                exit(1);
            }

            foreach my $file(@{$row->{'files'}}){
                    my @fs = split(/\//,$file->{'filename'});
                    my $filename = $fs[-1];
                    $sth_eva = $dbh_eva->prepare("insert into FILE (FILENAME,FILE_MD5,FILE_TYPE,ENA_SUBMISSION_FILE_ID,FILE_CLASS,FILE_VERSION,IS_CURRENT,FILE_LOCATION,FTP_FILE) values (?,?,?,?,?,?,?,?,?) returning file_id");
                    my $ftp_file_path = get_ftp_file_path($ena_ftp_file_prefix_path, $file_class, $row->{'ANALYSIS_ID'}, $filename);
                    $sth_eva->execute($filename,$file->{'MD5'},$file->{'filetype'},'0',$file_class,$file_version,1,$file_location,$ftp_file_path);
                    my $id = $sth_eva->fetchrow_hashref();
                    $sth_eva = $dbh_eva->prepare("insert into ANALYSIS_FILE (ANALYSIS_ACCESSION,FILE_ID) values (?,?)");
                    $sth_eva->execute($row->{'ANALYSIS_ID'},$id->{'file_id'});
                    my $eva_file_id;
                    if ($id->{'file_id'} < 10){
                        $eva_file_id = 'EVAF0000000'.$id->{'file_id'};
                    } elsif ($id->{'file_id'} < 100){
                        $eva_file_id = 'EVAF000000'.$id->{'file_id'};
                    } elsif ($id->{'file_id'} <1000){
                        $eva_file_id = 'EVAF00000'.$id->{'file_id'};
                    } elsif ($id->{'file_id'} <10000){
                        $eva_file_id = 'EVAF0000'.$id->{'file_id'};
                    } elsif ($id->{'file_id'} <100000){
                        $eva_file_id = 'EVAF000'.$id->{'file_id'};
                    } elsif ($id->{'file_id'} <100000){
                        $eva_file_id = 'EVAF00'.$id->{'file_id'};
                    } elsif ($id->{'file_id'} <1000000){
                        $eva_file_id = 'EVAF0'.$id->{'file_id'};
                    } elsif ($id->{'file_id'} <10000000){
                        $eva_file_id = 'EVAF'.$id->{'file_id'};
                    } else {
                        warn "Run out of EVAF accessions!";
                    }
                    if (($file->{'filetype'} =~ /^vcf$/i)||($file->{'filetype'} =~ /^vcf_aggregate$/i)){
                        $sth_eva = $dbh_eva->prepare("insert into browsable_file (file_id,ena_submission_file_id,filename,project_accession,loaded_assembly,assembly_set_id) values (?,?,?,?,?,?)");
                        $sth_eva->execute($id->{'file_id'},$eva_file_id,$filename,$project_accession,$assembly_code,$assembly_set);
                    }
                    $file_count++;
            }


    ### Insert into EVA_REFERENCED_SEQUENCE and ANALYSIS_SEQUENCE

            my $ref_name;
            $file_count = 0;
            foreach my $seq(@{$row->{'sequences'}}){
                    $sth_eva = $dbh_eva->prepare("insert into EVA_REFERENCED_SEQUENCE (SEQUENCE_ACCESSION,LABEL,REF_NAME) values (?,?,?) returning sequence_id");
                    $sth_eva->execute($seq->{'accession'},$seq->{'label'},$ref_name);
                    my $id = $sth_eva->fetchrow_hashref();
                    $sth_eva = $dbh_eva->prepare("insert into ANALYSIS_SEQUENCE (ANALYSIS_ACCESSION,SEQUENCE_ID) values (?,?)");
                    $sth_eva->execute($row->{'ANALYSIS_ID'},$id->{'sequence_id'});
                    $file_count++;
            }

            $file_count = 0;
            foreach my $exp(@{$row->{'experiment'}}){
                _insert_experiment($dbh_eva, $row->{'ANALYSIS_ID'}, $exp);
                $file_count++;
            }
        }
    }
}

sub _insert_experiment{
    my $dbh_eva = shift;
    my $analysis_accession = shift;
    my $experiment_type = shift;
    # Check if the analysis/experiment_id already have an entry
    $sth_eva = $dbh_eva->prepare("select ANALYSIS_ACCESSION, EXPERIMENT_TYPE_ID from ANALYSIS_EXPERIMENT_TYPE where ANALYSIS_ACCESSION=?");
    $sth_eva->execute($analysis_accession);
    my $analysis_from_experiment_id = "";
    while (my @row2 = $sth_eva->fetchrow_array()){
        $analysis_from_experiment_id = $row2[0];
    }
    if ($analysis_from_experiment_id eq ""){
        # Check if the Experiment type already exist
        $sth_eva = $dbh_eva->prepare("select experiment_type_id from EXPERIMENT_TYPE where EXPERIMENT_TYPE=?");
        $sth_eva->execute($experiment_type);

        my $experiment_type_id = -1;
        while (my $row2 = $sth_eva->fetchrow_hashref()){
            $experiment_type_id = $row2->{'experiment_type_id'};
        }
        # If not define it
        if ($experiment_type_id == -1){
            $sth_eva = $dbh_eva->prepare("insert into EXPERIMENT_TYPE (EXPERIMENT_TYPE) values (?) returning experiment_type_id");
            $sth_eva->execute($experiment_type);
            my $id = $sth_eva->fetchrow_hashref();
            $experiment_type_id = $id->{'experiment_type_id'};
        }
        $sth_eva = $dbh_eva->prepare("insert into ANALYSIS_EXPERIMENT_TYPE (ANALYSIS_ACCESSION,EXPERIMENT_TYPE_ID) values (?,?)");
        $sth_eva->execute($analysis_accession, $experiment_type_id);
    }
    return
}

sub _read_from_submission_xml{
    my $xml = shift;
    my $dbh = shift;
    my $parser = XML::LibXML->new();
    my $project_accession= shift;
    my $doc;
    my $sth_eva = $dbh->prepare("insert into SUBMISSION (SUBMISSION_ACCESSION,ACTION,TITLE,BROKERED,TYPE) values (?,?,?,?,?) returning submission_id");
    warn "Reading from $xml";
    eval{
        $doc = $parser->parse_file($xml);
    };
    if (!$doc){
        print "Failed to parse SUBMISSION_XML!";
    }
    my $row = {};
#   my $root = $doc->getDocumentElement;
    my $root = $doc->getDocumentElement;
    my $nodes = $root->find('/SUBMISSION_SET/SUBMISSION/ACTIONS/ACTION');
    foreach my $node(@$nodes){
        my @child_nodes = $node->nonBlankChildNodes();
        foreach my $child(@child_nodes){
            $row->{'ACTION'} = $child->nodeName;
        }

    }
    $nodes = $root->find('/SUBMISSION_SET/SUBMISSION');
    foreach my $node(@$nodes){
        $row->{'TITLE'} = $node->getAttribute('alias');
    }
    $nodes = $root->find('/SUBMISSION_SET/SUBMISSION');
    $row->{'ALIAS'} = $nodes->[0]->getAttribute('alias');
    $row->{'ALIAS'} = $nodes->[0]->getAttribute('center_name');
    my $max = 0;
    my $count = 1;
    if (!defined $row->{'SUBMISSION_ID'}){
        if ($max == 0){
            my $sth = $dbh->prepare("select submission_accession from SUBMISSION where submission_accession like \'ERAX%\' order by(submission_accession)");
            $sth->execute();
            while (my @res = $sth->fetchrow_array()){
#                       warn $res[0];
                $res[0] =~ s/ERAX//;
                $res[0] = $res[0] * 1;
#                       warn $res[0];
                if ($res[0] > $max){
                    $max = $res[0] * 1;
                }
#                       warn "Max is " . $max;
            }
        }
        warn "Creating dummy submission accession";
        my $acc_num = $max + $count;
        $count++;
        if ($acc_num < 10){
            $row->{'SUBMISSION_ID'} = 'ERAX0000'.$acc_num;
        } elsif ($acc_num < 100){
            $row->{'SUBMISSION_ID'} = 'ERAX000'.$acc_num;
        } elsif ($acc_num <1000){
            $row->{'SUBMISSION_ID'} = 'ERAX00'.$acc_num;
        } elsif ($acc_num <10000){
            $row->{'SUBMISSION_ID'} = 'ERAX0'.$acc_num;
        } elsif ($acc_num <100000){
            $row->{'SUBMISSION_ID'} = 'ERAX'.$acc_num;
        } else {
            warn "Run out of ERAX accessions!";
        }
        warn $row->{'SUBMISSION_ID'};
    } else {
        warn "Reading defined analysis accession - ". $row->{'SUBMISSION_ID'};
    }
    if (defined $row->{'UPDATED'}){
        if ($row->{'UPDATED'} =~ m/\s+(.*$)/){
            $row->{'UPDATED'} =~ s/\s+(.*$)//;
            $row->{'UPDATED'} = _oracle_to_mysql_date($row->{'UPDATED'});
            $row->{'UPDATED'} .= ' '.$1;
        }
    }
    $ena_submission_id = $row->{'SUBMISSION_ID'};
    $sth_eva->execute($row->{'SUBMISSION_ID'},$row->{'ACTION'},$row->{'TITLE'},1,'PROJECT');
    my $id = $sth_eva->fetchrow_hashref();

    push(@{$accession_to_id->{'SUBMISSION_ID'}},$id->{'submission_id'});

            warn Dumper($accession_to_id);
            $sth_eva = $dbh->prepare("insert into PROJECT_ENA_SUBMISSION (PROJECT_ACCESSION,SUBMISSION_ID) values (?,?)");
            foreach my $sub_id(@{$accession_to_id->{'SUBMISSION_ID'}}){
                $sth_eva->execute($project_accession,$sub_id);
            }
            $sth_eva = $dbh->prepare("insert into EVA_SUBMISSION (EVA_SUBMISSION_ID,EVA_SUBMISSION_STATUS_ID) values (?,?)");
        #   foreach my $sub_id(@{$accession_to_id->{'SUBMISSION_ID'}}){
                $sth_eva->execute($eload,6);
        #   }
            $sth_eva = $dbh_eva->prepare("insert into PROJECT_EVA_SUBMISSION (PROJECT_ACCESSION,old_ticket_id,ELOAD_ID) values (?,?,?)");
        #   foreach my $sub_id(@{$accession_to_id->{'SUBMISSION_ID'}}){
                $sth_eva->execute($project_accession,$eload,$eload);
        #   }
}

sub get_ftp_file_path
{
    my $prefix_path = shift;
    my $fileclass = shift;
    my $accession_id = shift;
    my $expected_prefix = "ERZ";
    if ($expected_prefix eq substr($accession_id,0,length($expected_prefix)) && $fileclass eq "submitted")
    {
        my $filename = shift;
        my $ftp_file_path = "$prefix_path/".substr($accession_id,0,6). "/$accession_id/$filename";

        return $ftp_file_path;
    }

    return undef; # Return a database NULL equivalent in Perl if the accession prefix is not ERZ
}

$dbh_eta->disconnect();
$dbh_ena->disconnect();