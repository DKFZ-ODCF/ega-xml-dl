# README sqlslurp

Reads a previously-downloaded XML dump into SQLite.

## Usage

```sh
./sqlslurp.py ega-box-NNN
```

The resulting SQLite database is placed in the XML directory: `$HOME/ega-xml/ega-box-NNN/box-contents.sqlite`.



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

