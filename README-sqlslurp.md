# README sqlslurp

This script can ingest an XML-dump of an EGA submitter box (as produced by its sister, `ega-xml-dl`)
into an SQLite database for easier querying.

The SQL tables follow the EGA naming scheme where possible, with two notable exceptions:
  1. given the many "IDs" and "accessions", the IDs used for crosslinking are named after their prefixes,
     which this author finds less-confusing and seems to be the only terminology that is consistent throughout the entire ecosystem.
     - `EGA*` for the ID's used for the public-facing identifiers we all know and love.
     - `ER*` for the internal representation, which seems to be a historical inheritance from the ENA/Webin days.
     - The star/`*` is one of:
       | Datatype | EGA* | ER* | Note                                            |
       |----------|:----:|:---:|-------------------------------------------------|
       | Study    | `S`  | `P` | from Study and Project, respectively            |
       | Sample   | `N`  | `S` | Careful to not confuse `ERS` with Study (`ERP`) |
       | Run      | `R`  | `R` |                                                 |
       | Analyses | `Z`  | `Z` |                                                 |
       | Dataset  | `D`  | `D` |                                                 |
  2. IDs referencing another datatype are prefixed with `XREF_`

## Usage

```sh
./sqlslurp.py ega-box-NNN
```

The resulting SQLite database is placed in the top level of the corresponding XML directory:
`$HOME/ega-xml/ega-box-NNN/box-contents.sqlite`.


## example queries

### Overview of all the "runs" in a Study

"Runs" are usually fastq files, but can also be (usually unaligned) bam-files,
as well as contain some other more exotic filetypes.
The guiding principle is that Runs should be "not interpreted", so free of human/processing biases/decisions.

Note that this query completely ignores any "analyses" that may or may not be part of the same dataset.

```sql
-- Details for fastq files (Runs) associated with a Study.
SELECT
  s.EGAS,
  n.submitter_id,
  n.EGAN,
  r.EGAR,
  r.forward_filename,
  r.forward_md5,
  r.reverse_filename,
  r.reverse_md5
FROM
  studies s
  LEFT JOIN experiments x ON x.XREF_ERP = s.ERP
  LEFT JOIN samples n     ON x.XREF_ERS = n.ERS
  LEFT JOIN runs r        ON r.XREF_ERX = x.ERX
WHERE
  s.EGAS = 'EGAS00001003953'
ORDER BY
  n.EGAN,
  r.EGAR;
```

