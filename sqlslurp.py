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
  return Path.home() / 'ega-xml' / which_box


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

    DROP TABLE IF EXISTS samples;
    CREATE TABLE samples (
        EGAN         varchar(15) PRIMARY KEY,
        ERS          varchar(10),
        submitter_id text NOT NULL,
        title        text,
        subject_id   text NOT NULL,
        gender       varchar(10) NOT NULL
    );
  """)

  return conn


def extract_study_info(path):
  """Extracts the 'interesting' elements from an EGA-study XML representation.

  Parameter path must be a pathlib Path to a study XML file.

  Returns a tuple of parsed fields, suitable for direct ingestion into the DB:
  (egas_id, )
  """

  log.debug("processing file %s", path)

  # extract EGAS-number from filename, since it appears nowhere in the XML.
  # XML internally uses ENA-style ERP ("Project")
  egas_id = path.name

  xml = ET.parse(path)

  erp_id = xml.find("./STUDY/IDENTIFIERS/PRIMARY_ID").text
  title = xml.find("./STUDY/DESCRIPTOR/STUDY_TITLE").text
  description = xml.find("./STUDY/DESCRIPTOR/STUDY_ABSTRACT").text

  # PubMed / PMID
  pubmed = xml.find("./STUDY/STUDY_LINKS/STUDY_LINK/XREF_LINK[DB='PUBMED']/ID")
  if pubmed != None:  # Pubmed is optional, extract text-only if present
    pubmed = pubmed.text

  result = (egas_id, erp_id, pubmed, title, description)
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
  egax_id = path.name

  xml = ET.parse(path)

  erx_id = xml.find("./EXPERIMENT/IDENTIFIERS/PRIMARY_ID").text
  xref_study_erp = xml.find("./EXPERIMENT/STUDY_REF/IDENTIFIERS/PRIMARY_ID").text

  xref_study_egas = xml.find("./EXPERIMENT/STUDY_REF/IDENTIFIERS/SECONDARY_ID")
  if xref_study_egas != None:
    xref_study_egas = xref_study_egas.text

  xref_sample_ers = xml.find("./EXPERIMENT/DESIGN/SAMPLE_DESCRIPTOR/IDENTIFIERS/PRIMARY_ID").text

  result = (egax_id, erx_id, xref_study_erp, xref_study_egas, xref_sample_ers )
  log.debug("  result: %s", result)
  return result


def extract_run_info(path):
  log.debug("processing file %s", path)

  egar_id = path.name

  xml = ET.parse(path)

  err_id = xml.find("./RUN/IDENTIFIERS/PRIMARY_ID").text
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

  result = (egar_id, err_id, xref_exp_erx, filetype, forward_filename, forward_md5, reverse_filename, reverse_md5 )
  log.debug("  result: %s", result)
  return result


def extract_sample_info(path):
  log.debug("processing file %s", path)

  egan_id = path.name

  xml = ET.parse(path)

  ers_id = xml.find("./SAMPLE/IDENTIFIERS/PRIMARY_ID").text
  submitter_id = xml.find("./SAMPLE/IDENTIFIERS/SUBMITTER_ID").text
  title = xml.find("./SAMPLE/TITLE").text
  subject_id = xml.find("./SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG='subject_id']/VALUE").text

  gender = xml.find("./SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG='gender']/VALUE")
  if gender == None:
    gender = xml.find("./SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG='sex']/VALUE")
  gender = gender.text

  result = (egan_id, ers_id, submitter_id, title, subject_id, gender )
  log.debug("  result: %s", result)
  return result


def process_dir(datatype, glob, extract_func, fieldcount, db_conn, box_dir):
  folder = box_dir / datatype
  raw_files = folder.glob(glob)
  parsed_files = ( extract_func(f) for f in raw_files )
  insert_sql = 'INSERT INTO %s VALUES ( %s );' % (datatype, ', '.join(['?'] * fieldcount))
  db_conn.executemany(insert_sql, parsed_files);
  db_conn.commit() # seems executemany implicitly starts a transaction that needs to be closed for data to show up.
  log.info("finished %s parsing", datatype)


def main():
  log.basicConfig( level=log.DEBUG, format='%(levelname)s: %(message)s' )

  box_dir = get_ega_xml_dir('ega-box-433')

  db_file = box_dir / dbName
  db_conn = create_or_open_db(db_file)

  log.info("slurping XMLs from %s into %s", box_dir, db_file)

  process_dir('studies',     'EGAS*', extract_study_info,  5, db_conn, box_dir)
  process_dir('experiments', 'EGAX*', extract_exp_info,    5, db_conn, box_dir)
  process_dir('runs',        'EGAR*', extract_run_info,    8, db_conn, box_dir)
  process_dir('samples',     'EGAN*', extract_sample_info, 6, db_conn, box_dir)

  db_conn.close()
  log.info("  DONE")
  return 0


if __name__ == '__main__':
  sys.exit(main())
