import urllib.request
import re
import logging
import sys
# from bs4 import BeautifulSoup
import psycopg2


# TODO incorporate this documentation
# def parse_cli():
#     parser = argparse.ArgumentParser(description="""
#      This script adds new assemblies and taxonomies to EVAPRO.
#      If you provide the assembly accession and the assembly code, the script will
#      try to add them linked to the official taxonomy as described in ENA.
#      Example assembly page: https://www.ebi.ac.uk/ena/data/view/GCA_000002315.3
#      You can also add the assembly with a different taxonomy if you provide the
#      taxonomy parameters. Example taxonomy page:
#      https://www.ebi.ac.uk/ena/data/view/Taxon:9031
#      """)
#     parser.add_argument('-a', '--accession', help='Assembly accession (Example: GCA_000002315.3)', required=True)
#     parser.add_argument('-c', '--code', help='Assembly code (Example: galgal5)', required=True)
#     parser.add_argument('--host', help='EVAPRO host', required=True)
#     parser.add_argument('-u', '--user', help='EVAPRO user', required=True)
#     parser.add_argument('-d', '--db-name', help='EVAPRO database name', required=True)
#     parser.add_argument('-t', '--taxonomy', help='Taxonomy id (Example: 9031)', required=False, type=int)
#     parser.add_argument('-e', '--eva-name', help='EVA name (Example: chicken) not required if the taxonomy exists or '
#                                                  'ENA has a common name available', required=False)
#     parser.add_argument('--dbsnp', help='Flag that this assembly load is from dbSNP',
#                         action='store_true', required=False)
#     parser.add_argument('--in-accessioning', help='Flag that this assembly is in the accessioning data store',
#                         action='store_true', required=False)
#     parser.add_argument('--in-variant-browser',
#                         help='Flag that variants from this assembly can be browsed from the EVA', action='store_true',
#                         required=False)
#     parser.add_argument('-s', '--simulate', help='Do a simulation run. Tell what the script is going to do, but don\'t '
#                                                  'write anything', action='store_true', required=False)
#     args = parser.parse_args()
#     if in_variant_browser and not dbsnp:
#         parser.error('--in-variant-browser switch can only be used with the --dbsnp argument '
#                      '(only applicable for assemblies imported from dbSNP)')
#     return args


def download_xml_from_ena(url):
    response = urllib.request.urlopen(url)
    return response.read()


def parse_ena_assembly_xml(assembly_accession):
    ena_assembly_url = 'https://www.ebi.ac.uk/ena/data/view/{}&display=xml'.format(assembly_accession)
    xml_content = download_xml_from_ena(ena_assembly_url)

    # TODO phase out BS
    soup = BeautifulSoup(xml_content, 'lxml')
    if soup.find('assembly') is None:
        logging.error('Assembly {} not found in ENA: {}'.format(assembly_accession, ena_assembly_url))
        sys.exit(1)
    assembly_name = soup.find('assembly')['alias']
    taxonomy_id = int(soup.find('taxon_id').contents[0])
    return assembly_name, taxonomy_id


def connect_db(host, user, dbname):
    conn = psycopg2.connect(host=host, user=user, dbname=dbname)
    return conn


def execute_query(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    # row = cur.fetchone()
    rows = []
    while True:
        row = cur.fetchone()
        if row is None:
            break
        rows.append(row)
    return rows


def is_in_db(conn, query, assembly_id):
    assembly_set_ids = execute_query(conn, query)
    if len(assembly_set_ids) > 0:
        set_id = str(assembly_set_ids[0][0])
        logging.warning('Assembly ' + assembly_id + ' is already in the database with assembly_set_id ' + set_id)
        logging.warning('No assemblies will be inserted')
        return True
    else:
        return False


def get_assembly_set(conn, taxonomy, assembly_accession):
    rows = execute_query(conn,
                         """SELECT acc.assembly_set_id 
                         FROM evapro.accessioned_assembly acc 
                         JOIN assembly_set asm on acc.assembly_set_id = asm.assembly_set_id 
                         WHERE assembly_accession=\'{}\' AND taxonomy_id={}""".format(assembly_accession, taxonomy))

    if len(rows) == 1:
        return rows[0][0]
    elif len(rows) == 0:
        return None
    else:
        raise Exception('Inconsistent database state: several assembly_set_ids for the same taxonomy ({}) and assembly '
                        'accession ({}): {}'.format(taxonomy, assembly_accession, rows))


def is_taxonomy_in_evapro(conn, taxonomy_id):
    taxonomy_query = 'SELECT taxonomy_id FROM evapro.taxonomy WHERE taxonomy_id={}'.format(taxonomy_id)
    taxonomy_ids_in_evapro = execute_query(conn, taxonomy_query)
    return len(taxonomy_ids_in_evapro) > 0


def parse_ena_taxonomy_xml(taxonomy):
    ena_taxonomy_url = 'https://www.ebi.ac.uk/ena/data/view/Taxon:{}&display=xml'.format(taxonomy)
    xml_content = download_xml_from_ena(ena_taxonomy_url)

    soup = BeautifulSoup(xml_content, 'lxml')
    xml_taxon = soup.find('taxon')
    if xml_taxon is None:
        raise Exception('Taxonomy {} not found in ENA: {}'.format(taxonomy, ena_taxonomy_url))
    scientific_name = xml_taxon['scientificname']
    optional_common_name = xml_taxon.get('commonname')
    return scientific_name, optional_common_name


def build_taxonomy_code(scientific_name):
    # Given a scientific name like "Zea mays", the corresponding taxonomy code should be zmays
    return (scientific_name.split(" ")[0][0] + scientific_name.split(" ")[1]).lower()


def ensure_taxonomy_is_in_evapro(conn, taxonomy, eva_name, simulate):
    if is_taxonomy_in_evapro(conn, taxonomy):
        logging.warning('Taxonomy {} is already in the database'.format(taxonomy))
    else:
        logging.info("Taxonomy {} not present in EVAPRO. Adding taxonomy ...".format(taxonomy))
        scientific_name, common_name = parse_ena_taxonomy_xml(taxonomy)
        taxonomy_code = build_taxonomy_code(scientific_name)
        eva_name = eva_name if eva_name is not None else common_name
        if eva_name is None:
            raise Exception(
                'The taxonomy in ENA doesn\'t include a common name. '
                'Please specify the EVA name for the species "{}"'.format(scientific_name))
        insert_taxonomy(conn, taxonomy, scientific_name, common_name, taxonomy_code, eva_name, simulate)


def insert_assembly(conn, taxonomy_id, assembly_accession, assembly_name, assembly_code, simulate=False):
    if simulate:
        logging.info('Simulate inserting assembly set: INSERT INTO evapro.assembly_set('
                     'taxonomy_id, assembly_name, assembly_code) VALUES ({}, {}, {})'
                     .format(taxonomy_id, assembly_name, assembly_code))
        logging.info('Simulate inserting accessioned assembly: INSERT INTO evapro.accessioned_assembly('
                     'assembly_set_id, assembly_accession, assembly_chain, assembly_version) '
                     'VALUES ({}, {}, {}, {})'.format('<assembly_set_id>', assembly_accession, '<assembly_chain>',
                                                      '<assembly_version>'))
        return -1
    else:
        cur = conn.cursor()

        cur.execute('INSERT INTO evapro.assembly_set(taxonomy_id, assembly_name, assembly_code) VALUES (%s, %s, %s)',
                    (taxonomy_id, assembly_name, assembly_code))

        # get the assembly_set_id that was autogenerated in the row that we just inserted in assembly_set
        assembly_set_id = execute_query(conn,
                                        'SELECT assembly_set_id FROM evapro.assembly_set '
                                        'WHERE taxonomy_id={} and assembly_name=\'{}\' and assembly_code=\'{}\''
                                        .format(taxonomy_id, assembly_name, assembly_code))[0][0]

        assembly_chain = assembly_accession.split('.')[0]
        assembly_version = assembly_accession.split('.')[1]
        cur.execute('INSERT INTO evapro.accessioned_assembly('
                    'assembly_set_id, assembly_accession, assembly_chain, assembly_version) VALUES (%s,%s,%s,%s)',
                    (assembly_set_id, assembly_accession, assembly_chain, assembly_version))

        logging.info('New assembly added with assembly_set_id: {0}'.format(assembly_set_id))
        return assembly_set_id


def update_accessioning_status(conn, taxonomy_id, assembly_set_id, assembly_accession, eva_taxonomy_name,
                               dbsnp_flag, in_variant_browser_flag, in_accessioning_flag, simulate=False):
    cur = conn.cursor()
    eva_taxonomy_name_from_db = execute_query(conn,
                                              "SELECT eva_name FROM evapro.taxonomy "
                                              "WHERE taxonomy_id = {0}".format(taxonomy_id))
    eva_taxonomy_name_from_db = None if len(eva_taxonomy_name_from_db) == 0 else eva_taxonomy_name_from_db[0][0]
    if not (eva_taxonomy_name or eva_taxonomy_name_from_db):
        raise Exception('Error: Taxonomy code and EVA taxonomy name are required for inserting a taxonomy')
    eva_taxonomy_name = eva_taxonomy_name or eva_taxonomy_name_from_db

    if dbsnp_flag:
        dbsnp_assemblies_insert_query = "INSERT INTO evapro.dbsnp_assemblies " \
            "SELECT * FROM (" \
            "SELECT cast('dbsnp_{0}_{1}' as text) as database_name" \
            ", {2} as assembly_set_id" \
            ", cast('{3}' as text) as assembly_accession" \
            ", cast('{4}' as boolean) as loaded) temp " \
            "WHERE (database_name, assembly_set_id, loaded) NOT IN " \
            "(SELECT database_name, assembly_set_id, loaded FROM evapro.dbsnp_assemblies)".format(
                re.sub(r'\s+', '', eva_taxonomy_name).strip(), taxonomy_id, assembly_set_id,
                assembly_accession, in_variant_browser_flag)
        cur.execute(dbsnp_assemblies_insert_query) if not simulate \
            else logging.info("Simulating insert into evapro.dbsnp_assemblies: " + dbsnp_assemblies_insert_query)

    # Only insert assembly accessions which are NOT already in the assembly_accessioning_store_status table
    assembly_accessioning_store_insert_query = "INSERT INTO evapro.assembly_accessioning_store_status " \
                                               "SELECT * FROM (SELECT " \
                                               "cast('{0}' as text) as assembly_accession" \
                                               ", cast('{1}' as boolean) as loaded) temp " \
                                               "WHERE assembly_accession NOT IN " \
                                               "(SELECT assembly_accession FROM " \
                                               "evapro.assembly_accessioning_store_status)" \
                                               .format(assembly_accession, in_accessioning_flag)
    cur.execute(assembly_accessioning_store_insert_query) if not simulate \
        else logging.info("Simulating insert into evapro.assembly_accessioning_store_status: " +
                          assembly_accessioning_store_insert_query)


def insert_taxonomy(conn, taxonomy_id, scientific_name, common_name, taxonomy_code, eva_name, simulate=False):
    if taxonomy_code is None or eva_name is None:
        raise Exception('Error: taxonomy code ({}) and EVA taxonomy name ({}) are required '
                        'for inserting a taxonomy'.format(taxonomy_code, eva_name))
    if simulate:
        logging.info('Simulate inserting taxonomy: insert into evapro.taxonomy('
                     'taxonomy_id, common_name, scientific_name, taxonomy_code, eva_name) VALUES ({}, {}, {}, {}, {})'
                     .format(taxonomy_id, common_name, scientific_name, taxonomy_code, eva_name))
    else:
        cur = conn.cursor()
        cur.execute('INSERT INTO evapro.taxonomy(taxonomy_id, common_name, scientific_name, taxonomy_code, eva_name) '
                    'VALUES (%s, %s, %s, %s, %s)',
                    (taxonomy_id, common_name, scientific_name, taxonomy_code, eva_name))

    logging.info('New taxonomy {} added'.format(taxonomy_id))


def insert_new_assembly_and_taxonomy(
        assembly_accession,
        assembly_code,
        pg_host,
        pg_user,
        db_name,
        taxonomy=None,
        eva_name=None,
        dbsnp=False,
        in_accessioning=False,
        in_variant_browser=False,
        simulate=False
):
    conn = None
    try:
        # check if assembly is already in EVAPRO, adding it if not
        conn = connect_db(pg_host, pg_user, db_name)

        assembly_name, official_taxonomy = parse_ena_assembly_xml(assembly_accession)
        if taxonomy is not None:
            taxonomy = taxonomy
            if taxonomy != official_taxonomy:
                logging.warning("Adding taxonomy {} for assembly {}, although the standard taxonomy is {}".format(
                    taxonomy, assembly_accession, official_taxonomy))
        else:
            taxonomy = official_taxonomy

        assembly_set_id = get_assembly_set(conn, taxonomy, assembly_accession)
        if assembly_set_id is not None:
            logging.warning("Assembly set id {} already links taxonomy {} and assembly {}".format(assembly_set_id,
                                                                                                  taxonomy,
                                                                                                  assembly_accession))
        else:
            ensure_taxonomy_is_in_evapro(conn, taxonomy, eva_name, simulate)
            assembly_set_id = insert_assembly(conn, taxonomy, assembly_accession, assembly_name, assembly_code,
                                              simulate)

        update_accessioning_status(conn, taxonomy, assembly_set_id, assembly_accession, eva_name,
                                   dbsnp, in_variant_browser, in_accessioning, simulate)

        conn.commit()
    finally:
        if conn is not None:
            conn.close()
