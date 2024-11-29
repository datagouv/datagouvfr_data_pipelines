from datetime import datetime, timedelta
from dateutil.parser import parse
import requests
import pandas as pd

from datagouvfr_data_pipelines.config import (
    SECRET_SENTRY_API_TOKEN,
    SENTRY_BASE_URL,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message

max_csvgz_size = 104857600
organization_slug = 'sentry'
project_slug = 'hydra'
headers = {
    'Authorization': f'Bearer {SECRET_SENTRY_API_TOKEN}'
}


def get_error(analysis_parsing_error):
    issue_id = analysis_parsing_error.split(':')[-1]
    url = f'{SENTRY_BASE_URL}api/0/projects/{organization_slug}/{project_slug}/events/{issue_id}/'
    r = requests.get(url, headers=headers)
    try:
        r.raise_for_status()
        return r.json()['entries'][0]['data']['values'][0]['type']
    except:
        return None


def better_parse(date):
    try:
        return parse(date)
    except:
        return None


def get_delay_seconds(start, end):
    if any(not isinstance(_, datetime) for _ in [start, end]):
        return None
    return (end - start).total_seconds()


def build_resource_url(dataset_id, resource_id):
    return f"https://www.data.gouv.fr/fr/datasets/{dataset_id}/#/resources/{resource_id}"


def get_and_send_errors():
    print('Getting all datasets of meteo.data.gouv...')
    catalog = requests.get(
        'https://www.data.gouv.fr/api/1/topics/6571f222129681e83de11aa2/',
        headers={"X-fields": "datasets{id}"}
    ).json()['datasets']

    print('Getting infos for all resources...')
    data = []
    for d in catalog:
        r = requests.get(
            f"https://www.data.gouv.fr/api/1/datasets/{d['id']}/",
            headers={"X-fields": "resources{id,extras,filesize,format,type,internal,preview_url,title}"}
        ).json()
        for res in r['resources']:
            if 'csv' in res['format'] and res['type'] == 'main':
                row = {
                    "dataset_id": d['id'],
                    'rid': res['id'],
                    'title': res['title'],
                    'filesize': res['filesize'],
                    'preview_url': res['preview_url'],
                    'internal:last_modified': better_parse(res['internal']['last_modified_internal']),
                    "analysis:last-modified-at": better_parse(res['extras'].get("analysis:last-modified-at")),
                    "analysis:parsing:error": res['extras'].get("analysis:parsing:error"),
                    "analysis:parsing:finished_at": better_parse(res['extras'].get("analysis:parsing:finished_at")),
                    "analysis:parsing:started_at": better_parse(res['extras'].get("analysis:parsing:started_at")),
                }
                data.append(row)
    df = pd.DataFrame(data)
    for c in [
        'internal:last_modified',
        'analysis:last-modified-at',
        'analysis:parsing:finished_at',
        'analysis:parsing:started_at'
    ]:
        df[c] = pd.to_datetime(df[c], utc=True)
    df['parsing_time'] = df.apply(
        lambda x: get_delay_seconds(x['analysis:parsing:started_at'], x['analysis:parsing:finished_at']),
        axis=1
    )
    df['delay_to_parse'] = df.apply(
        lambda x: get_delay_seconds(x['internal:last_modified'], x['analysis:parsing:started_at']),
        axis=1
    )

    should_be_previz = df.loc[df['filesize'] <= max_csvgz_size]
    is_previz = should_be_previz.loc[~should_be_previz['preview_url'].isna()]
    no_info = should_be_previz.loc[
        should_be_previz['preview_url'].isna()
        & should_be_previz['analysis:parsing:error'].isna()
    ]
    delay = df.loc[
        (df['delay_to_parse'] >= 0)
        & (df['internal:last_modified'] >= better_parse('2024-04-01 00:00:00+00:00')),
        'delay_to_parse'
    ]
    sentry_error = should_be_previz.loc[~should_be_previz['analysis:parsing:error'].isna()]
    print('Getting infos from sentry...')
    sentry_error['error'] = sentry_error['analysis:parsing:error'].apply(get_error)
    errors = sentry_error['error'].value_counts(dropna=False).to_dict()

    print('Creating report...')
    message = "#### Statistiques hydra :handshake: météo\n"
    message += f"{round(len(is_previz) / len(should_be_previz) * 100, 1)}% de ressources prévisualisables (< taille max) "
    message += f"\n({len(is_previz)}/{len(should_be_previz)} fichiers)\n"
    message += "##### Durée d'analyse des fichiers :\n"
    message += f"- moyenne : {round(df['parsing_time'].mean(), 1)}s\n"
    message += f"- médiane : {round(df['parsing_time'].median(), 1)}s\n"
    message += "##### Délai entre maj et analyse :\n"
    message += f"- moyenne : {timedelta(seconds=delay.mean())}\n"
    message += f"- médiane : {timedelta(seconds=delay.median())}s\n"
    message += f"##### {sum(errors.values())} erreurs :\n"
    for e in errors:
        message += f"- `{e or 'Inconnue'}`, {errors[e]} cas, exemples :\n"
        if e is None:
            restr = sentry_error.loc[sentry_error['error'].isnull()]
        else:
            restr = sentry_error.loc[sentry_error['error'] == e]
        if len(restr) > 5:
            restr = restr.sample(5)
        for _, row in restr.iterrows():
            i = row['analysis:parsing:error'].split(':')[-1]
            message += (
                f"   - [{row['title']}]({build_resource_url(row['dataset_id'], row['rid'])}), "
                f"[Erreur Sentry]({SENTRY_BASE_URL}organizations/sentry/issues/143874/events/{i}/)\n"
            )
    message += "\n\n Ressources sans aucune info :\n"
    for _, row in no_info.iterrows():
        message += (
            f"- [{row['title']}]({build_resource_url(row['dataset_id'], row['rid'])})\n"
        )
    send_message(message)
