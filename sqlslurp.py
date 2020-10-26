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

  conn.execute(
  """CREATE TABLE IF NOT EXISTS studies (
	EGAS varchar(15) PRIMARY KEY,
	ERP varchar(10),
	PMID integer,
	title text NOT NULL
      );"""
  )

  return conn


def extractStudyInfo(path):
  """Extracts the 'interesting' elements from an EGA-study XML representation.
  
  Parameter path must be a pathlib Path to a study XML file.
  
  Returns a dict with the fields of interest.
  """

  log.debug("processing file %s", path)

  result = dict()

  # extract EGAS-number from filename, since it appears nowhere in the XML.
  # XML internally uses ENA-style ERP ("Project")
  egasNumber = path.name
  result['egasNumber'] = egasNumber
  
  xml = ET.parse(path)

  # extract title
  title = xml.find("./STUDY/DESCRIPTOR/STUDY_TITLE").text
  result['title'] = title
  
  # ENA ERP number
  erpNumber = xml.find("./STUDY/IDENTIFIERS/PRIMARY_ID").text
  result['erpNumber'] = erpNumber
  
  # PubMed / PMID
  pubmed = xml.find("./STUDY/STUDY_LINKS/STUDY_LINK/XREF_LINK[DB='PUBMED']/ID")
  if pubmed != None:  # Pubmed is optional, extract text-only if present
    pubmed = pubmed.text
  result['pubmed'] = pubmed
  
  log.debug("result for %s: %s", egasNumber, result)


def save_study_info(fields):
  """Saves the extracted study information into the sqlite DB.
  """

  pass # TODO


def process_studies(db_conn):
  studiesDir = boxDir / 'studies'

  for s in studiesDir.glob('EGAS*'):
    result = extractStudyInfo(s)
    save_study_info(result)
    break  # DEBUG: finish after first study, that's enough while dev'ing



def main():
  log.basicConfig( level=log.DEBUG, format='%(levelname)s: %(message)s' )

  boxDir = getEgaXmlDir('ega-box-433')
  db_file = boxDir / dbName
  log.info("slurping XMLs from %s into %s", boxDir, db_file)
  
  db_conn = create_or_open_db(db_file)

  process_studies(db_conn)
    
  return 0


if __name__ == '__main__':
  sys.exit(main())
