# EGA XML downloader

This simple scripts lets you batch-download the contents of an ENA/EGA submission box at the EBI.
It fetches the XML representation of all objects in the submission box.

## Usage

```sh 
# for ega-boxes
bash ega-xml-dl ega-box-NNN
# for ENA boxes
bash ega-xml-dl Webin-NNNNN
```

If you don't provide the box as command line parameter, you'll be asked for the ega-box number. Enter only the digits, the "ega-box-" part is automatically
added. E.g. for "ega-box-000" enter only "000" when prompted, like so:

```sh
# without a command line argument
> bash ega-xml-dl
From which box do you wish to download? ega-box-_
> 81
```

The box password is asked per normal `read -p` on stdin, to avoid passwords ending up in shell history or (externally visible) shell environment.

## Caching strategy

The script is (only just) intelligent enough to only download entries that aren't already available locally. Unfortunately it is dumb enough that it
won't detect _changed_ files. These are not downloaded again. To fetch a completely new state (e.g. after updating old items), first clear the corresponding
output folder in `$HOME/ega-xml`. Optionally, you can only remove those objects which you know are changed, to avoid
redownloading everything.

## Feedback welcome

This script started out as (and still is) a small internal tool at the Omics IT & Datamanagement Core Facility at the German Cancer Research Centre (DKFZ),
a publicly funded body. It is made openly available under the MIT license under the philosophy of ["public money, public code"](https://publiccode.eu/)

If you use it, and have any ideas, suggestions or wish to contribute improvements, feel free to contribute or open issues at upstream:

https://gitlab.com/DKFZ-ODCF/ega-xml-dl

## semi-documented API details

This download script uses two semi-documented APIs provided by EGA/ENA.
These were reverse-engineered by looking at what the Webin interface does "under the hood".

Both endpoints require authentication using http basic auth.
The endpoint accepts both ENA login (animal data, `webin-00000`) and EGA accounts (human data, `ega-box-000`).

Both API are implementation details of the Webin platform, so could change at any time.
(Although it has been stable for multiple years, so far..)
Depend on them at your own risk!

### 'report' endpoint

The 'report' endpoint gives one an overview of all IDs available in a given box.

```
https://www.ebi.ac.uk/ena/submit/report/$TYPE/?format=csv&max-results=99999
```

Where `$TYPE` is one of `analyses experiments runs samples studies submissions dacs datasets`.

### 'drop-box' endpoint

The 'drop-box' is primarily used for programmatic XML submissions, but also exposes previously submitted data
using the Sequence Read Archive SRA XML format.
For more information and examples on the SRA XML format, please see 
[https://ega-archive.org/submission/sequence/programmatic_submissions/prepare_xml](https://ega-archive.org/submission/sequence/programmatic_submissions/prepare_xml).

```
https://www.ebi.ac.uk/ena/submit/drop-box/$TYPE/$ID?format=xml%
```

with `$TYPE` as above, and `$ID` an EGA-style ID from the CSV-report, e.g. `EGAC00001000452` (dac), or 
`EGAS00001003554` (study).

Note that the returned XMLs sometimes use ENA-style IDs internally (e.g. `EGAS00001003554` -> `ERP114444`), 
which complicates mapping.
