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

def getEgaXmlDir(whichBox):
  """Returns the canonical path to the cache for `whichBox`.
  
  Parameter `whichBox` should be an EGA or ENA login, e.g. "ega-box-000" or "Webin-00000"
  """
  return Path.home() / 'ega-xml' / whichBox


def create_or_open_db(db_file):
  conn = sqlite3.connect(db_file)

  conn.executescript(
  """DROP TABLE IF EXISTS studies;
    CREATE TABLE studies (
	EGAS varchar(15) PRIMARY KEY,
	ERP varchar(10),
	PMID integer,
	title text NOT NULL,
	description text NOT NULL
      );"""
  )

  return conn


def extractStudyInfo(path):
  """Extracts the 'interesting' elements from an EGA-study XML representation.
  
  Parameter path must be a pathlib Path to a study XML file.
  
  Returns a tuple of parsed fields, suitable for direct ingestion into the DB:
  (egas_number, )
  """

  log.debug("processing file %s", path)

  # extract EGAS-number from filename, since it appears nowhere in the XML.
  # XML internally uses ENA-style ERP ("Project")
  egas_number = path.name
  
  xml = ET.parse(path)

  # extract title
  title = xml.find("./STUDY/DESCRIPTOR/STUDY_TITLE").text
  
  # ENA ERP number
  erp_number = xml.find("./STUDY/IDENTIFIERS/PRIMARY_ID").text
  
  # PubMed / PMID
  pubmed = xml.find("./STUDY/STUDY_LINKS/STUDY_LINK/XREF_LINK[DB='PUBMED']/ID")
  if pubmed != None:  # Pubmed is optional, extract text-only if present
    pubmed = pubmed.text
  
  description = 'todo'

  result = (egas_number, erp_number, pubmed, title, description)
  log.debug("  result: %s", result)
  return result


def process_studies(db_conn, box_dir):
  studies_dir = box_dir / 'studies'

  studies_files = studies_dir.glob('EGAS*')
  parsed_studies = ( extractStudyInfo(f) for f in studies_files )
  insert_study_sql = "INSERT INTO studies VALUES (?, ?, ?, ?, ?);"

  db_conn.executemany(insert_study_sql, parsed_studies);


  log.info("finished study parsing")


def main():
  log.basicConfig( level=log.DEBUG, format='%(levelname)s: %(message)s' )

  box_dir = getEgaXmlDir('ega-box-433')

  db_file = box_dir / dbName
  db_conn = create_or_open_db(db_file)

  log.info("slurping XMLs from %s into %s", box_dir, db_file)

  process_studies(db_conn, box_dir)

  db_conn.commit()
  db_conn.close()

  return 0


if __name__ == '__main__':
  sys.exit(main())
