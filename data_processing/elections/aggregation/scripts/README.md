# Preparing the sources of the `data_processing_elections` DAG

The DAG does not build the standardized files itself: it concatenates the ones listed
in [`../sources.json`](../sources.json), stored on our S3 under
`elections/sources/<key>/{general,candidats}-results.csv`. These scripts, run by hand
outside Airflow, build those files and keep `sources.json` up to date.

This folder is excluded from Airflow parsing (`../.airflowignore`).

## `sources.json`

One entry per source dataset of the Ministère de l'Intérieur:

- `id_elections`: the `id_election` values contained in the files (a file may hold
  both rounds, e.g. `2001_cant` → `2001_cant_t1`, `2001_cant_t2`);
- `source_dataset_id`: the original dataset of the Ministère de l'Intérieur on
  data.gouv.fr;
- `source_last_update`: the `last_update` of that dataset when our files were built;
- `files`: the S3 paths.

On every run, the DAG task `check_sources_updates` alerts on Tchap if a source dataset
is unreachable, archived, or modified after `source_last_update`.

The first 41 entries come from a one-shot migration (September 2026) of the files
previously published as community resources, whose building code is not in this
repository.

`2012_legi` and `2012_pres`: accented characters restored on 2026-10-01 from the
original MIOM cp1252 files (they had been read as utf-8, turning accents into `?`).

## Running a script

Environment variables: `S3_ENDPOINT`, `S3_BUCKET`, `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, and optionally `S3_REGION` (defaults to `sbg`; without a
region, OVH rejects signed reads). From `dags/`:

    uv run --no-project --with pandas --with openpyxl --with xlrd --with requests --with boto3 \
      python -m datagouvfr_data_pipelines.data_processing.elections.aggregation.scripts.build_senatoriales [--year 2026] [--dry-run --output-dir /tmp/sena]

`--dry-run` only writes the files locally, without uploading to S3 nor editing
`sources.json`: use it to check a change before applying it.

## When the DAG reports a modified source dataset

1. Compare the source dataset with our files.
2. If the correction must be taken into account: for the senatorial elections, run
   `build_senatoriales.py --year <year>` again, which also updates
   `source_last_update`; for the other elections, whose code is not here, rebuild the
   files and upload them to the same S3 path.
3. Otherwise, or after step 2 for the other elections, copy the dataset's
   `last_update` into `source_last_update` by hand.

## Senatorial elections (`build_senatoriales.py`)

General renewals from 1992 to 2026, one round per `id_election` (`YYYY_sena_t1`,
`YYYY_sena_t2`). In the first round, the departments voting under the majoritarian
system and those voting under the proportional system share the same `id_election`
(each department uses a single voting system).

- **Department level**: `inscrits` = grands électeurs; `code_commune`, `code_bv` and
  `code_circonscription` are empty. In 2023, the ministry locates the vote at the
  prefecture commune and a nominal polling station: this is not kept, so as not to
  assign all the electors of a department to its prefecture.
- **The ministry's department codes are kept** (`ZA`, `ZZ`, `971`...), as for the other
  elections; only one-digit codes are padded (`3` → `03`).
- **Blank and null votes are merged before 2014**: the total is in `nuls`, `blancs`
  is empty (same convention as the 2008 municipal elections).
- **Votes**: in plurinominal majoritarian departments each elector votes for several
  candidates, so the sum of `voix` in a department can exceed `exprimes`.
- **Not kept**: elected candidates and seats (not in the schema, and missing from the
  2020 file), filing numbers; `no_panneau` is empty.
- **Harmonizations**: `sexe` as `M`/`F` (2023 and 2026 spell out
  `MASCULIN`/`FEMININ`), title removed from `nom_tete_liste` (2020), exact duplicate
  rows removed (2014: Saint-Martin, Wallis-et-Futuna, Saint-Barthélemy).
- **Known discrepancy in the source**: Paris 2023, the list votes add up to 2,837 for
  2,836 `exprimes`.

## Elections not included: partial elections

The partial elections published by the ministry on data.gouv.fr are deliberately
excluded for now: they are summary sheets per constituency or per commune, without
blank or null votes, not comparable to the results per polling station.

| Election | Source dataset |
|---|---|
| Senatorial partial elections, 6 September 2015 (Cantal, Gers) | `55eeae9188ee387fdda46ec2` |
| Municipal partial elections, 14 and 21 June 2015 (5 communes) | `5588119dc751df484ba453ba` |
| Legislative partial elections, 13 and 20 March 2016 (Aisne, Nord, Yvelines) | `57037563c751df64d8c485cb` |
| Legislative partial election, 17 and 24 April 2016 (Loire-Atlantique 3rd) | `5727336788ee383b9fa19f12` |
| Legislative partial elections, 22 and 29 May 2016 (Alpes-Maritimes, Bas-Rhin) | `574c467c88ee383dcbd1b934` |
| Legislative partial election, 5 and 12 June 2016 (Ain 3rd) | `575eaabc88ee380e51640391` |

The other partial elections are only published on the Ministère de l'Intérieur
website.
