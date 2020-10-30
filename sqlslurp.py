#!/usr/bin/env python3

import logging as log
import sys
import sqlite3
from pathlib import Path
import xml.etree.ElementTree as ET


# Hardcoded basedir to match the default output location from the ega-xml-dl shell-script
basedir = "~/ega-xml/"

# filename for the sqlite result that will be written in the box dir
dbName = 'box-contents.sqlite'

def get_ega_xml_dir(which_box):
  """Returns the canonical path to the cache for `which_box`.

  Parameter `which_box` should be an EGA or ENA login, e.g. "ega-box-000" or "Webin-00000"
  """

  box_dir = Path.home() / 'ega-xml' / which_box

  if ( not box_dir.is_dir() ):
    raise FileNotFoundError("couldn't find XML dump directory for box: '%s'" % box_dir).with_traceback(None)

  return box_dir


def create_or_open_db(db_file):
  conn = sqlite3.connect(db_file)

  conn.executescript("""
    DROP TABLE IF EXISTS studies;
    CREATE TABLE studies (
	EGAS        varchar(15) PRIMARY KEY,
	ERP         varchar(10),
	PMID        integer,
	title       text NOT NULL,
	description text NOT NULL
    );

    DROP TABLE IF EXISTS experiments;
    CREATE TABLE experiments (
        EGAX      varchar(15) PRIMARY KEY,
        ERX       varchar(10),
        XREF_ERP  varchar(10) NOT NULL,
        XREF_EGAS varchar(15),
        XREF_ERS  varchar(10) NOT NULL
    );

    DROP TABLE IF EXISTS runs;
    CREATE TABLE runs (
        EGAR      varchar(15) PRIMARY KEY,
        ERR       varchar(10),
        XREF_ERX  varchar(10) NOT NULL,
        filetype  varchar(5)  NOT NULL,
        forward_filename  text NOT NULL,
        forward_md5       varchar(32) NOT NULL,
        reverse_filename  text,
        reverse_md5       varchar(32)
    );

    DROP TABLE IF EXISTS analyses;
    CREATE TABLE analyses (
        EGAZ      varchar(15) PRIMARY KEY,
        ERZ       varchar(10),
        XREF_ERP  varchar(10) NOT NULL,
        XREF_EGAS varchar(15),
        XREF_ERS  varchar(10) NOT NULL,
        filetype  varchar(5)  NOT NULL,
        filename  text,
        md5       varchar(32)
    );

    DROP TABLE IF EXISTS samples;
    CREATE TABLE samples (
        EGAN         varchar(15) PRIMARY KEY,
        ERS          varchar(10),
        submitter_id text NOT NULL,
        title        text,
        subject_id   text NOT NULL,
        gender       varchar(10) NOT NULL
    );

    DROP TABLE IF EXISTS datasets;
    CREATE TABLE datasets (
        EGAD        varchar(15) PRIMARY KEY,
        EGAP        varchar(15) NOT NULL,
	title       text NOT NULL,
	description text
    );
    DROP TABLE IF EXISTS datasets_runs;
    CREATE TABLE datasets_runs (
        EGAD varchar(15),
        EGAR varchar(15),
        PRIMARY KEY (EGAD, EGAR)
    );
    DROP TABLE IF EXISTS datasets_analyses;
    CREATE TABLE datasets_analyses (
        EGAD varchar(15),
        EGAZ varchar(15),
        PRIMARY KEY (EGAD, EGAZ)
    );
  """)

  return conn


def extract_study_info(path):
  """Extracts the 'interesting' elements from an EGA-study XML representation.

  Parameter path must be a pathlib Path to a study XML file.

  Returns a tuple of parsed fields, suitable for direct ingestion into the DB:
  (ega_id, )
  """

  log.debug("processing file %s", path)

  # extract EGAS-number from filename, since it appears nowhere in the XML.
  # XML internally uses ENA-style ERP ("Project")
  ega_id = path.name

  xml = ET.parse(path)

  ena_id = xml.find("./STUDY/IDENTIFIERS/PRIMARY_ID").text
  title = xml.find("./STUDY/DESCRIPTOR/STUDY_TITLE").text
  description = xml.find("./STUDY/DESCRIPTOR/STUDY_ABSTRACT").text

  # PubMed / PMID
  pubmed = xml.find("./STUDY/STUDY_LINKS/STUDY_LINK/XREF_LINK[DB='PUBMED']/ID")
  if pubmed != None:  # Pubmed is optional, extract text-only if present
    pubmed = pubmed.text

  result = (ega_id, ena_id, pubmed, title, description)
  log.debug("  result: %s", result)
  return result


def extract_exp_info(path):
  """Extracts the 'interesting' elements from an EGA experiment XML representation.

  Parameter path must be a pathlib Path to an experiment XML file.

  Returns a tuple of parsed fields, suitable for direct ingestion into the DB:
  (egax, erx, xref_study_erp, xref_study_egas, xref_sample_ers)
  """

  log.debug("processing file %s", path)

  # extract EGAX-number from filename, since it appears nowhere in the XML.
  # XML internally uses ENA-style ERX
  ega_id = path.name

  xml = ET.parse(path)

  ena_id = xml.find("./EXPERIMENT/IDENTIFIERS/PRIMARY_ID").text
  xref_study_erp = xml.find("./EXPERIMENT/STUDY_REF/IDENTIFIERS/PRIMARY_ID").text

  xref_study_egas = xml.find("./EXPERIMENT/STUDY_REF/IDENTIFIERS/SECONDARY_ID")
  if xref_study_egas != None:
    xref_study_egas = xref_study_egas.text

  xref_sample_ers = xml.find("./EXPERIMENT/DESIGN/SAMPLE_DESCRIPTOR/IDENTIFIERS/PRIMARY_ID").text

  result = (ega_id, ena_id, xref_study_erp, xref_study_egas, xref_sample_ers )
  log.debug("  result: %s", result)
  return result


def extract_run_info(path):
  log.debug("processing file %s", path)

  ega_id = path.name

  xml = ET.parse(path)

  ena_id = xml.find("./RUN/IDENTIFIERS/PRIMARY_ID").text
  xref_exp_erx = xml.find("./RUN/EXPERIMENT_REF/IDENTIFIERS/PRIMARY_ID").text

  files = xml.findall("./RUN/DATA_BLOCK/FILES/FILE")
  filetype = files[0].get('filetype')

  # "forward" file; there should be at least one file
  # either a R1/forward, or a bam-file
  forward_md5 = files[0].get('unencrypted_checksum')
  forward_filename = files[0].get('filename')

  # reverse file, if available, defaults to 'None' for single-end sequencing or bams
  reverse_md5 = None
  reverse_filename = None
  if len(files) == 2:
    reverse_md5 = files[1].get('unencrypted_checksum')
    reverse_filename = files[1].get('filename')

  result = (ega_id, ena_id, xref_exp_erx, filetype, forward_filename, forward_md5, reverse_filename, reverse_md5 )
  log.debug("  result: %s", result)
  return result


def extract_analyses_info(path):
  log.debug("processing file %s", path)

  ega_id = path.name

  xml = ET.parse(path)

  ena_id = xml.find("./ANALYSIS/IDENTIFIERS/PRIMARY_ID").text
  submitter_id = xml.find("./ANALYSIS/IDENTIFIERS/SUBMITTER_ID").text
  title = xml.find("./ANALYSIS/TITLE").text
  xref_study_erp = xml.find("./ANALYSIS/STUDY_REF/IDENTIFIERS/PRIMARY_ID").text

  xref_study_egas = xml.find("./ANALYSIS/STUDY_REF/IDENTIFIERS/SECONDARY_ID")
  if xref_study_egas != None:
    xref_study_egas = xref_study_egas.text

  xref_sample_ers = xml.find("./ANALYSIS/SAMPLE_REF/IDENTIFIERS/PRIMARY_ID").text

  file = xml.find("./ANALYSIS/FILES/FILE")
  filetype = file.get('filetype')
  filename = file.get('filename')
  file_md5 = file.get('unencrypted_checksum')

  result = (ega_id, ena_id, xref_study_erp, xref_study_egas, xref_sample_ers, filetype, filename, file_md5)
  log.debug("  result: %s", result)
  return result


def extract_sample_info(path):
  log.debug("processing file %s", path)

  ega_id = path.name

  xml = ET.parse(path)

  ena_id = xml.find("./SAMPLE/IDENTIFIERS/PRIMARY_ID").text
  submitter_id = xml.find("./SAMPLE/IDENTIFIERS/SUBMITTER_ID").text
  title = xml.find("./SAMPLE/TITLE").text
  subject_id = xml.find("./SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG='subject_id']/VALUE").text

  gender = xml.find("./SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG='gender']/VALUE")
  if gender == None:
    gender = xml.find("./SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG='sex']/VALUE")
  gender = gender.text

  result = (ega_id, ena_id, submitter_id, title, subject_id, gender )
  log.debug("  result: %s", result)
  return result


def extract_dataset_info(path):
  log.debug("processing file %s", path)

  ega_id = path.name

  xml = ET.parse(path)
  policy = xml.find("./DATASET/POLICY_REF/IDENTIFIERS/PRIMARY_ID").text
  title = xml.find("./DATASET/TITLE").text

  description = xml.find("./DATASET/DESCRIPTION")
  if description != None:
    description = description.text

  run_refs = xml.findall('./DATASET/RUN_REF/IDENTIFIER/PRIMARY_ID')
  run_links = ( (ega_id, ref.text) for ref in run_refs )
  analyses_refs = xml.findall('./DATASET/ANALYSIS_REF/IDENTIFIER/PRIMARY_ID')
  analyses_links = ( (ega_id, ref.text) for ref in analyses_refs )

  # 3-tuple of 'normal result', 'runs' and 'analyses'
  # with the latter two in a way that can be fed directly into `executemany`
  result = (
    (ega_id, policy, title, description ),
    run_links,
    analyses_links
  )
  log.debug("  result: %s", result)
  return result

def process_datasets(db_conn, box_dir):
  """Process datasets separately, since they contain one-to-many relationships (1 dataset -> N runs and/or M analyses)."""

  folder = box_dir / 'datasets'
  raw_files = folder.glob('EGAD*')
  parsed_files = ( extract_dataset_info(f) for f in raw_files )

  for (result, run_links, analyses_links) in parsed_files:
    db_conn.execute('INSERT INTO datasets VALUES (?, ?, ?, ?);', result)
    db_conn.executemany('INSERT INTO datasets_runs VALUES (?, ?);', run_links)
    db_conn.executemany('INSERT INTO datasets_analyses VALUES (?, ?);', analyses_links)

  db_conn.commit() # seems executemany implicitly starts a transaction that needs to be closed for data to show up.
  log.info("finished datasets parsing")


def process_dir(datatype, glob, extract_func, fieldcount, db_conn, box_dir):
  """Metafunction to slurp all XML-files into a directory into the sql DB

  Paramaters:
    datatype: String representing both the XML-directory name and the SQL-table into which to insert
              options: studies, experiments, runs, samples, analyses, datasets, dacs

    glob: The filename glob pattern that is followed by the XML filenames.
          Having this protects against accidental stray files in the XML-directory

    extract_func: function handle to the XML-parsing function that translates the XML-contents into
                  a tuple of only the 'interesting' stuff.
                  See the various `extract_FOO_info` functions.

    fieldcount: Number of fields returned by `extract_func`, so the SQL knows how many values to insert.

    db_conn: Opened connection to the SQLite DB.

    box_dir: Pathlib `Path` to the root of the XML-dump we are parsing; used to construct the datatype sub-paths.

  Returns: None
  """

  folder = box_dir / datatype
  raw_files = folder.glob(glob)
  parsed_files = ( extract_func(f) for f in raw_files )
  insert_sql = 'INSERT INTO %s VALUES ( %s );' % (datatype, ', '.join(['?'] * fieldcount))
  db_conn.executemany(insert_sql, parsed_files);
  db_conn.commit() # seems executemany implicitly starts a transaction that needs to be closed for data to show up.
  log.info("finished %s parsing", datatype)


def main(which_box):
  log.basicConfig( level=log.DEBUG, format='%(levelname)s: %(message)s' )

  box_dir = get_ega_xml_dir(which_box)

  db_file = box_dir / dbName
  db_conn = create_or_open_db(db_file)

  log.info("slurping XMLs from %s into %s", box_dir, db_file)

  process_dir('studies',     'EGAS*', extract_study_info,    5, db_conn, box_dir)
  process_dir('experiments', 'EGAX*', extract_exp_info,      5, db_conn, box_dir)
  process_dir('runs',        'EGAR*', extract_run_info,      8, db_conn, box_dir)
  process_dir('analyses',    'EGAZ*', extract_analyses_info, 8, db_conn, box_dir)
  process_dir('samples',     'EGAN*', extract_sample_info,   6, db_conn, box_dir)
  process_datasets(db_conn, box_dir)

  db_conn.close()
  log.info("  DONE")
  return 0


if __name__ == '__main__':
  which_box = sys.argv[1]
  sys.exit(main(which_box))
