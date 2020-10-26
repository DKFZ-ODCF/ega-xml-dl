#!/usr/bin/env python3

import logging as log
import sys
from pathlib import Path
import xml.etree.ElementTree as ET


# Hardcoded basedir to match the default output location from the ega-xml-dl shell-script
basedir = "~/ega-xml/"


def getEgaXmlDir(whichBox):
  """Returns the canonical path to the cache for `whichBox`.
  
  Parameter `whichBox` should be an EGA or ENA login, e.g. "ega-box-000" or "Webin-00000"
  """
  return Path.home() / 'ega-xml' / whichBox


def extractStudyInfo(path):
  """extracts the 'interesting' elements from an EGA-study XML representation.
  
  Parameter path must be a pathlib Path to a study XML file.
  """

  log.debug("processing file %s", s)
  
  egasNumber = path.name
  
  xml = ET.parse(path)
  print("title:", xml.find(".//STUDY_TITLE").text)


def main():
  log.basicConfig( level=log.DEBUG, format='%(levelname)s: %(message)s' )

  boxDir = getEgaXmlDir('ega-box-433')
  log.info("slurping XMLs from %s", boxDir)
  
  studiesDir = boxDir / 'studies'
  for s in studiesDir.glob('EGAS*'):
    extractStudyInfo(s)
    break  # DEBUG: finish after first study, that's enough while dev'ing
    
    return 0


if __name__ == '__main__':
  sys.exit(main())
