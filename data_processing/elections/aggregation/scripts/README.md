# Preparing the sources of the `data_processing_elections` DAG

The DAG does not build the standardized files itself: it concatenates the ones listed
in [`../sources.json`](../sources.json), stored on our S3 under
`elections/sources/<key>/` (`general-results.csv`, `candidats-results.csv`, and
`nuances.csv` when the election has a nuance grid). These scripts, run by hand
outside Airflow, build those files and keep `sources.json` up to date.

This folder is excluded from Airflow parsing (`../.airflowignore`).

## `sources.json`

One entry per election source, with:

- `id_elections`: the `id_election` values contained in the files (a file may hold
  both rounds, e.g. `2001_cant` → `2001_cant_t1`, `2001_cant_t2`);
- `resultats`: the results files;
- `description`: how the election is listed in the "Sources des données agrégées"
  section of the dataset description (`libelle`, optional `commentaire`), see below;
- `nuances` (optional): the nuance grid of the election, one row per nuance and per
  `id_election` (`type_nuance`, `bloc`, `nuance`, `signification`,
  `commentaires`, `source`; see `dtypes["nuances"]` in `../schema.py`).

Each part has:

- `source_dataset_id`: the original dataset of the Ministère de l'Intérieur on
  data.gouv.fr, empty when there is none (the nuance grids come from the ministry's
  circulars, cited in their `source` column);
- `source_last_update`: the `last_update` of that dataset when our files were built;
- `files`: the S3 paths.

On every run, the DAG task `check_sources_updates` alerts on Tchap if a source dataset
is unreachable, archived, or modified after `source_last_update` (parts without a
`source_dataset_id` are skipped).

## Dataset description

The dataset description is built by the DAG task `publish_description`: the manual
text of [`../description.yaml`](../description.yaml), then the list of the sources, one
line per entry of `sources.json` (`description.libelle`, linked to its source dataset,
followed by `description.commentaire` in brackets), most recent first. Edit the
description there, not in the data.gouv UI: the DAG overwrites it when it changes.

Entries **without `resultats`** are elections that are not integrated (no data per
polling station, partial elections...): they are struck through in the list, and their
`description` holds their own `source_dataset_id`. The DAG steps that read the results
skip them.

The nuance grids of `2026_muni_t1` and `2026_muni_t2` come from a one-shot migration
(October 2026) of the dataset resource "Dictionnaire des nuances politiques
(circulaire INTP2602966C de février 2026)", copied as is.

The other former resource, "Dictionnaire des nuances politiques (2025)" (86 codes,
`Nuance` and `Libellé` only, compiled from the ministry's results archives for all
elections at once, so not attributable to a given election), is not used. It is kept
for the record, as is, at `elections/archives/dictionnaire-nuances-2025.csv` on our
S3, outside `sources.json`.

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

## Adding a new election

When the Ministère de l'Intérieur publishes the results of a new election (or when a
past one is to be integrated):

1. **Find the source dataset(s)** of the ministry on data.gouv.fr: the final results
   per polling station ("résultats définitifs par bureau de vote"), usually one dataset
   per round. Note their ids.
2. **Write a build script in this folder** (e.g. `build_legislatives.py`), committed, on
   the model of `build_senatoriales.py`: it downloads the original files, parses them
   and writes the standardized files, with a `--dry-run` option. Use the helpers of
   `common.py` (`download_file`, `upload_file`, `get_dataset`, `register_source`).
   The standardized files must follow the schemas of `../schemas/`:
   - `general-results.csv` and `candidats-results.csv`, `;`-separated, UTF-8; only
     columns of `schema-general-results.json` / `schema-candidats-results.json` (the
     DAG adds the missing ones as empty), and the DAG check `check_outputs` rejects any
     value that doesn't match the schema types;
   - `id_election` as `année_type_tour` (e.g. `2027_legi_t1`); `id_brut_miom` as
     `<code_commune>_<code_bv>` like the other elections (`01001_0001`); 5-character
     INSEE `code_commune`; the ministry's department codes kept as is (`ZA`, `ZZ`,
     `971`...);
   - recompute the ratios from the counts, and check the encoding of the original files
     (the 2012 files had been read with the wrong one, turning accents into `?`).
3. **Run it with `--dry-run`**, check the output (rows per round, totals against the
   ministry's national results, a few polling stations by hand), then run it for real:
   it uploads the files to `elections/sources/<key>/` and registers the entry in
   `sources.json` (`id_elections`, `resultats` with `source_dataset_id` and
   `source_last_update`).
4. **Add the `description` part** of the new entry in `sources.json`: `libelle` as in
   the other entries (e.g. "Législatives 2027 T1") and, if needed, a `commentaire`
   (coverage limits, merged columns...). It appears in the dataset description at the
   next run.
5. **Nuance grid**: transcribe the grid of the ministry's circular on the attribution of
   nuances for this election (Légifrance), with a double check against the document
   since the PDFs are scans; write `nuances.csv` following `schema-nuances-mapping.json`
   (`source` = "Circulaire <NOR> du <date>"), upload it next to the results and add a
   `nuances` part to the entry (`source_dataset_id` empty). Check that every `nuance`
   code of the new results is in the grid.
6. **New election type** (not one of `pres`, `legi`, `euro`, `regi`, `dpmt`, `cant`,
   `muni`, `sena`): add it to `TYPES_ORDER` in `../description.py` (order of the list of
   sources) and to the list of types in the introduction of `../description.yaml`.
7. **Run the DAG in dev** with all the steps: `check_outputs` must pass, and
   `process_communes` reports on Tchap the communes it can't map (an election of a year
   not covered yet by the INSEE annual table is mapped on its latest millesime). Check
   the outputs on demo.
8. **Add a line to the history** in `../description.yaml` (e.g. "JJ/MM/AAAA : ajout des
   élections législatives de 2027"), then deploy and run in prod.

An election that is **not integrated** (no data per polling station, partial
election...) can still be listed, struck through, in the dataset description: add an
entry without `resultats`, whose `description` has `libelle`, `source_dataset_id` and a
`commentaire` giving the reason (see `2001_muni` or `2016_03_legi_part`).

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

## Production release checklist (feat/improve-dag-elections)

Steps to follow, in this order, when this branch goes to production. Everything was
validated on demo first. Dataset: `6481e741d4cf002ec0efec9d` on www.data.gouv.fr.

### 1. Before deploying the code

- [ ] **Create the new resources on the prod dataset**, all as **remote links** (not
  uploaded files: a resource type can't be changed afterwards, neither in the UI nor
  through the API), and copy their titles and descriptions from demo (they are managed in
  the UI, the DAG only sends `url`, `filesize` and `format`):

  | Resource | Type | Format | `config.json` key to fill |
  |---|---|---|---|
  | Table de correspondance avec les communes actuelles | main | csv | `communes.csv.prod` |
  | Table de correspondance des nuances par élection | main | csv | `nuances.csv.prod` |
  | Schéma de données - Résultats généraux | documentation | json | `general.schema.prod` |
  | Schéma de données - Résultats par candidat | documentation | json | `candidats.schema.prod` |
  | Schéma de données - Table de correspondance avec les communes actuelles | documentation | json | `communes.schema.prod` |
  | Schéma de données - Table de correspondance des nuances par élection | documentation | json | `nuances.schema.prod` |

- [ ] **Replace the six `"TODO"`** of `config.json` with these resource ids (otherwise
  `publish_results_elections` fails in prod).
- [ ] **Fill in the history** in `description.yaml`: the exact date and content of the
  "01/2026" line, and the release date instead of `JJ/MM/2026`. The first prod run
  overwrites the dataset description with this file.
- [ ] **Merge the branch.** No S3 action is needed: the sources under `elections/sources/`
  are shared by dev and prod, and the 2025 nuance dictionary is already archived at
  `elections/archives/dictionnaire-nuances-2025.csv` (checked identical to the original).

### 2. First production run

- [ ] Trigger the DAG manually with **all the steps** (default `steps` param).
- [ ] Check that `check_outputs` passed, then on the dataset: the 6 data resources and the
  4 schema resources point to `…/elections/…` and `…/elections/schemas/…`, with their sizes,
  and the description shows the generated list of sources and the history.
- [ ] Expect a Tchap alert from `check_sources_updates` for `2026_muni_t1` (its source
  dataset was updated on 2026-03-20, after our files): review it and update
  `source_last_update` in `sources.json` once handled.

### 3. After the first successful run

- [ ] **Remove the resources replaced by the new ones** (only now, so that the dataset
  never lacks them):

  | Resource | Id | Replaced by |
  |---|---|---|
  | `schema-general-results.json` (uploaded file) | `ced4d21f-9d17-4224-94c0-d0bf0bc28b1c` | the linked general schema |
  | `schema-candidats-results.json` (uploaded file) | `c702d75b-4e7e-43c9-84fa-83220302931d` | the linked candidats schema |
  | Dictionnaire des nuances politiques (circulaire INTP2602966C de février 2026) | `15b3e0e5-396b-4423-9556-faed8a52bfa2` | the nuances table |
  | Dictionnaire des nuances politiques (2025) | `6fd17a6c-519b-465c-a7fd-ad2955fafc76` | nothing (archived on S3) |

- [ ] Archive the parquet resources of the results (`ff16d511…`, `4d3b35f6…`) when
  decided: data.gouv.fr now builds a parquet export from the CSV.
- [ ] Publish the release note to users. Changes visible in the published data:
  - senatorial elections 1992–2026 added, at department level (`inscrits` = grands
    électeurs, `id_brut_miom` = department code, no commune nor polling station);
  - `liste` column removed from the candidats file, its content (2020_muni_t2) moved to
    `libelle_etendu_liste`;
  - accented characters of 2012 (`2012_legi`, `2012_pres`) restored: commune labels and
    candidates' first and last names;
  - rows now ordered by election (chronological key order);
  - new correspondence table to the current communes (`table_passage_communes.csv`) and
    new nuances table (`nuances_politiques.csv`, municipales 2026 for now, column
    `nuance` as in the candidats file);
  - schemas 0.0.4 for the results (`"float"` → `"number"`, no `liste`, `nuance` name
    fixed) and new schemas for the two tables, all published as links; the nuance
    dictionaries are removed.

### Optional clean-up (dev)

- [ ] Delete the dev S3 orphans left by renamings: `dev/elections/schemas/schema-communes-results.json`
  and `dev/elections/schemas/schema-nuances-results.json` (now `…-mapping.json`).
