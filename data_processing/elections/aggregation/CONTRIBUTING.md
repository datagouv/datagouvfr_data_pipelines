# Preparing the sources of the `data_processing_elections` DAG

The DAG does not build the standardized files itself: it concatenates the ones listed
in [`data/sources.json`](data/sources.json), stored on our S3 under
`elections/sources/<key>/` (`general-results.csv`, `candidats-results.csv`, and
`nuances.csv` when the election has a nuance grid). The scripts of
[`scripts/`](scripts/), run by hand outside Airflow, build those files and keep
`sources.json` up to date.

`scripts/` and `tests/` are excluded from Airflow parsing (`.airflowignore`).

## `sources.json`

One entry per election source, with:

- `id_elections`: the `id_election` values contained in the files (a file may hold
  both rounds, e.g. `2001_cant` → `2001_cant_t1`, `2001_cant_t2`);
- `resultats`: the results files;
- `description`: how the election is listed in the "Sources des données agrégées"
  section of the dataset description (`libelle`, `date` of the (first) round, optional
  `commentaire`), see below;
- `nuances` (optional): the nuance grid of the election, one row per nuance and per
  `id_election` (`type_nuance`, `bloc`, `nuance`, `signification`,
  `commentaires`, `source`; see `dtypes["nuances"]` in `utils/schema.py`).

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
text of [`data/description.yaml`](data/description.yaml), then the list of the sources, one
line per entry of `sources.json` (`description.libelle`, linked to its source dataset,
followed by `description.commentaire` in brackets), sorted by `description.date`, most
recent first. The dates come from the source datasets' titles or descriptions, or the
ministry's results archive pages, and from the French Wikipedia page of each election for
the ones before 2014 (checked in October 2026). Edit the
description there, not in the data.gouv UI: the DAG overwrites it when it changes.

Entries **without `resultats`** are elections that are not integrated (no data per
polling station, partial elections...): they are struck through in the list, and their
`description` holds their own `source_dataset_id`, or none when the election is not
published on data.gouv.fr (listed without a link). The DAG steps that read the results
skip them. The general elections from 1992 to 1998 are listed this way, so that the
list covers the same period as the senatorial elections (1992 onwards): results per
commune at best, and the régionales 1992 and municipales 1995 not published at all.

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
2. **Write a build script in `scripts/`** (e.g. `build_legislatives.py`), committed, on
   the model of `build_senatoriales.py`: it downloads the original files, parses them
   and writes the standardized files, with a `--dry-run` option. Use the helpers of
   `common.py` (`download_file`, `upload_file`, `get_dataset`, `register_source`).
   The standardized files must follow the schemas of `data/schemas/`:
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
   the other entries (e.g. "Législatives 2027 T1"), the `date` of the round
   (`AAAA-MM-JJ`, the first round for an entry holding both) and, if needed, a
   `commentaire` (coverage limits, merged columns...). It appears in the dataset
   description at the next run. For a new election type (not one of `pres`, `legi`,
   `euro`, `regi`, `dpmt`, `cant`, `muni`, `sena`), also add it to the list of types in
   the introduction of `data/description.yaml`.
5. **Nuance grid**: transcribe the grid of the ministry's circular on the attribution of
   nuances for this election (Légifrance), with a double check against the document
   since the PDFs are scans; write `nuances.csv` following `schema-nuances-mapping.json`
   (`source` = "Circulaire <NOR> du <date>"), upload it next to the results and add a
   `nuances` part to the entry (`source_dataset_id` empty). Check that every `nuance`
   code of the new results is in the grid.
6. **Run the DAG in dev** with all the steps: `check_outputs` must pass, and
   `process_communes` reports on Tchap the communes it can't map (an election of a year
   not covered yet by the INSEE annual table is mapped on its latest millesime). Check
   the outputs on demo.
7. **Add value tests** for the new election in `tests/expected_values.yaml` (see
   [Value tests](#value-tests)): a few rows read by hand in the ministry's files,
   e.g. one polling station with its counts and the votes of one candidate.
8. **Add a line to the history** in `data/description.yaml` (e.g. "JJ/MM/AAAA : ajout des
   élections législatives de 2027"), then deploy and run in prod.

An election that is **not integrated** (no data per polling station, partial
election...) can still be listed, struck through, in the dataset description: add an
entry without `resultats`, whose `description` has `libelle`, `date`,
`source_dataset_id` and a `commentaire` giving the reason (see `2001_muni` or `2016_03_legi_part`).

## Value tests

`tests/expected_values.yaml` lists values read by hand in the original publications
of the ministry (the sources need parsing, so they can't be checked automatically). Each
case selects rows of `general_results.csv` or `candidats_results.csv` with `filtres`
(`id_election` required) and gives the `source` where the values were read, with
either:

- `valeurs`: the values of the **single row** selected, e.g. a polling station;
- `agregats`: per column, aggregates over **all the rows** selected, e.g. a
  department: `somme`, `moyenne`, `min`, `max`, `nombre` (of non-empty values).

A single row:

```yaml
- table: candidats
  filtres:
    id_election: "2022_pres_t1"
    id_brut_miom: "75101_0001"
    nom: "MACRON"
  valeurs:
    voix: 321
    ratio_voix_exprimes: 33.44
  source: "Présidentielle 2022 T1, résultats par bureau de vote, ligne …"
```

A department:

```yaml
- table: general
  filtres:
    id_election: "2022_pres_t1"
    code_departement: "37"
  agregats:
    inscrits:
      somme: 450123
    code_bv:
      nombre: 812
    ratio_votants_inscrits:
      moyenne: 78.12
  source: "Présidentielle 2022 T1, résultats par département, Indre-et-Loire"
```

- the codes must be quoted (`"01"`, `"01001_0001"`), otherwise YAML reads them as
  numbers and the case finds no row;
- numbers are compared at the precision written in the YAML (`33.44` → 2 decimals);
- a `valeurs` case matching no row or several rows, or an `agregats` case matching no
  row, is an error.

They run at each run of the DAG, in `check_outputs`, so that adding or rebuilding an
election can't silently change the values of the others: a mismatch fails the run
before anything is sent or published. They can also be run on a produced file, from
`dags/`:

```bash
python -m datagouvfr_data_pipelines.data_processing.elections.aggregation.tests.check_values candidats /path/to/candidats_results.csv
```

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

## Cantonal and departmental elections (`cant`, `dpmt`)

Both elect the members of the departmental council (called "conseil général" until 2015),
but with different voting systems:

- **Cantonales** (`cant`, 2001, 2004, 2008, 2011 integrated; 1992, 1994, 1998 listed
  only): single-member majoritarian ballot in two rounds, **one** councillor elected per
  canton, in `nom` / `prenom`. From 2008 (law of 31 January 2007), each candidate runs
  with a substitute of the other sex, who only sits if the seat becomes vacant: still one
  person elected.
- **Renewal by half**: the councils were renewed by half every three years, in two series
  of cantons (1992, 1998, 2004, 2011 for one; 1994, 2001, 2008 for the other). Each
  cantonal election therefore covers about half of the territory: around 18,500 communes
  per election in our data, and only 277 to 358 communes in both series of consecutive
  elections (communes split between cantons of both series, e.g. large cities).
- **Départementales** (`dpmt`, 2015 and 2021, next in 2028): since 2015, each canton
  (redrawn, about half as many) elects a **pair** of a woman and a man, both elected and
  sitting; the pair is in `binome`, `nom` / `prenom` are empty. Full renewal every six
  years, so each election covers the whole territory (around 35,000 communes).

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

  | Resource | Type | Format | `data/destinations.json` key to fill |
  |---|---|---|---|
  | Table de correspondance avec les communes actuelles | main | csv | `communes.csv.prod` |
  | Table de correspondance des nuances par élection | main | csv | `nuances.csv.prod` |
  | Schéma de données - Résultats généraux | documentation | json | `general.schema.prod` |
  | Schéma de données - Résultats par candidat | documentation | json | `candidats.schema.prod` |
  | Schéma de données - Table de correspondance avec les communes actuelles | documentation | json | `communes.schema.prod` |
  | Schéma de données - Table de correspondance des nuances par élection | documentation | json | `nuances.schema.prod` |

- [ ] **Replace the six `"TODO"`** of `data/destinations.json` with these resource ids (otherwise
  `publish_results_elections` fails in prod).
- [ ] **Fill in the history** in `data/description.yaml`: the exact date and content of the
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

## Next steps

Improvements and corrections identified but not done yet. Remove an item once done, and
mention it in the release note if it changes the published data.

### List labels in the candidats file

`libelle_abrege_liste`, `libelle_etendu_liste` and `nom_tete_liste` are empty for most
list elections. Checked in October 2026 against the published candidats file (dev,
2026-10-06) and the ministry's datasets on data.gouv.fr:

| Election | Labels available in | Join key tested | Match |
|---|---|---|---|
| `2008_muni_t1`, `t2` | our own source `560521ca88ee3865ae5b59f0` (`Liste` column, lost when our files were built) | – | – |
| `2020_muni_t1` | our own source `5e7102d5ebe5d838b7581662` (`Liste` column, lost) | – | – |
| `2021_regi_t1`, `t2` | our own sources `60d1c3d14fc2e0fda6816678`, `60dd933d9bcd72926212eeec` (abrégé, étendu, tête de liste, lost) | – | – |
| `1999_euro_t1` | `53699387a3a729239d2041ff` (results by region, 20 lists) | head of list "NOM Prénom" | 100 % |
| `2014_euro_t1` | `5369938aa3a729239d204208` ("Fichier des listes et des candidats", abrégé + étendu) | head of list (accents removed) | 99.1 % |
| `2004_regi_t1`, `t2` | `536993a5a3a729239d204258` (results by region, with head of list) | nuance + head of list (name + first initial) | 100 % |
| `2010_regi_t1`, `t2` | `536993a8a3a729239d204260` (results by region, abrégé + étendu) | same | 98.8 % / 98.0 % |
| `2015_regi_t1`, `t2` | `56a0a31d88ee380692822419`, `56a0a797c751df41b1ade713` (results by commune) | department + head of list | 99.5 % / 100 % |
| `2014_muni_t1`, `t2` (lists only) | `5369939aa3a729239d20423c`, `5369939da3a729239d204241` (results of communes ≥ 1,000 inhabitants, `Liste`) | department + commune + head of list | 99.99 % / 99.93 % |
| `2004_euro_t1`, `2009_euro_t1` | none: `Libellé Abrégé Liste` of their result files only holds the nuance label (12–14 values for 160+ lists) | – | – |

- The remaining mismatches are spelling variants, to be handled when building: double
  spaces (`LE PEN  Marine`), `LEPAPE` / `LE PAPE`, truncated or extended names (`DE GOUYON
  MATIGNON DE PONTOURAUDE`); use the filing number (`Code Dépôt`) for 2014 euro, and the
  first name for the two homonymous heads of list of La Réunion 2015 (`ROBERT`, `PAYET`).
- The 2014 euro "Résultats" datasets (`53893403a3a7291ffe8c5032`…) also only hold nuance
  labels; their real labels are limited to the lists with elected members ("Elus" sheet).
  Same for 2015 regi: the candidatures datasets (`5642354e88ee387dc2e72046`,
  `56684270c751df2f56c664bc`) hold the labels too.
- Paris, Lyon and Marseille are coded by sector in the 2014 municipal files
  (`056SR12`), and Mayotte communes as `5xx` instead of `6xx`.
- Several source datasets will then have to be cited for one election: `sources.json`
  needs a way to list them (e.g. a `listes` part with its own `source_dataset_id` and
  `source_last_update`, so that `check_sources_updates` and the dataset description cover
  it).
- Make the use of the label columns consistent: today they are filled differently per
  election (2019 euro: abrégé + tête; 2024 euro and 2026 muni: abrégé + étendu; 2020 muni
  T2: étendu only; senatorial: depends on the year).

### Head of list and type of ballot in the candidats file

Where the head of list is stored depends on the election (checked in October 2026 on the
dev candidats file):

| Head of list in | Elections |
|---|---|
| `nom_tete_liste` only, `nom` / `prenom` empty | `2019_euro_t1`; senatorial T1 1992–2020 (proportional departments) |
| `nom` / `prenom`, `nom_tete_liste` empty | euro 1999–2014; regi 2004–2021 (e.g. `2015_regi_t1`); muni 2008, 2014, 2020; `2026_muni_t2` |
| nowhere (`nom`, `prenom` and `nom_tete_liste` empty) | `2024_euro_t1`, `2026_muni_t1`, senatorial 2023 and 2026 lists: to check whether their sources give it |

The départementales have no list but a pair of candidates: `nom` / `prenom` are empty and
the pair is in `binome`.

Proposed target, to be discussed before changing the schema:

- `nom` / `prenom` hold the head of list when the row is a list and the information is
  available, as for a single candidate; `nom_tete_liste` is no longer needed (move its
  values to `nom` / `prenom`, splitting "NOM Prénom" on the case: upper-case words are the
  name, capitalized ones the first name).
- A dedicated column says what the row is: the type of ballot (list proportional, list
  majoritarian, uninominal, binomial...) and whether it is a list or not. One column or
  two depends on the values: if the type of ballot always implies list or not, one column
  is enough; it doesn't for the municipales before 2026 (lists in the communes of 1,000
  inhabitants and more, individual candidates below) nor the senatorial elections
  (majoritarian or proportional per department), where the value is per row anyway.
- Schema change (new version), release note, and the value tests to update.
- `2020_muni_t2`: same list spelled with different cases (`ENSEMBLE POUR PARIS AVEC AGNÈS
  BUZYN` / `Ensemble pour Paris avec Agnès Buzyn`).
- The building code of these elections is not in this repository (see `sources.json`):
  fixing them means writing their build scripts first.

### Other data checks and corrections

- **`2026_muni_t1`** source dataset updated on 2026-03-20, after our files: to review
  (see the production checklist).
- **2026 municipal, French Polynesia (987)**: 933 rows without list nor nuance, with a
  candidate name; to check against the ministry's file (specific voting system?).
- **`2026_muni_t1`, `86063_0001`**: one row with votes (65) but no candidate, nuance nor
  list.
- **`2009_euro`**: 3 odd rows (nuances LDD / LDV / LPC, `01001_0001`) to check against
  the source.
- **Senatorial elections**: check that the first-round departments without list are
  exactly those voting under the majoritarian system each year.
- **2026 nuances table**: the `commentaires` lost their punctuation in the original
  resource.

### Further improvements

- Nuance grids for the other elections, transcribed from the ministry's circulars
  (Légifrance); only the municipales 2026 one is in `sources.json` for now.
- Partial elections (see [above](#elections-not-included-partial-elections)), if a
  comparable format becomes available.
- Enrichments of the aggregated data, e.g. boundaries ("contours").

### Clean-up

- Archive the parquet resources of the results (`ff16d511…`, `4d3b35f6…`) when decided:
  data.gouv.fr now builds a parquet export from the CSV.
- Delete the dev S3 orphans left by renamings: `dev/elections/schemas/schema-communes-results.json`
  and `dev/elections/schemas/schema-nuances-results.json` (now `…-mapping.json`).
