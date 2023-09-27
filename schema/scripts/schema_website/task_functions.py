import yaml
import git
from git import Repo, Git
import os
import shutil
from table_schema_to_markdown import convert_source
import frictionless
import json
import jsonschema
import re
from unidecode import unidecode
import codecs
import requests
from urllib import parse
import numpy as np

ERRORS_REPORT = []
SCHEMA_INFOS = {}
SCHEMA_CATALOG = {}


def initialization(ti, TMP_FOLDER):
    # DAG_NAME is defined in the DAG file, and all paths are made from it
    OUTPUT_DATA_FOLDER = f"{TMP_FOLDER}output/"
    CACHE_FOLDER = TMP_FOLDER + 'cache'
    DATA_FOLDER1 = TMP_FOLDER + 'data'
    DATA_FOLDER2 = TMP_FOLDER + 'data2'
    folders = {
        'OUTPUT_DATA_FOLDER': OUTPUT_DATA_FOLDER,
        'CACHE_FOLDER': CACHE_FOLDER,
        'DATA_FOLDER1': DATA_FOLDER1,
        'DATA_FOLDER2': DATA_FOLDER2,
    }

    branch = 'main'
    if 'preprod' in TMP_FOLDER:
        branch = 'preprod'
    LIST_SCHEMAS_YAML = f"https://raw.githubusercontent.com/etalab/schema.data.gouv.fr/{branch}/repertoires.yml"

    # Loading yaml file containing all schemas that we want to display in schema.data.gouv.fr
    r = requests.get(LIST_SCHEMAS_YAML)

    with open(TMP_FOLDER + 'repertoires.yml', 'wb') as f:
        f.write(r.content)

    with open(TMP_FOLDER + 'repertoires.yml', "r") as f:
        config = yaml.safe_load(f)

    ti.xcom_push(key='folders', value=folders)
    ti.xcom_push(key='config', value=config)


def clean_and_create_folder(folder):
    """Remove local folder if exist and (re)create it"""
    if os.path.exists(folder):
        shutil.rmtree(folder)
    os.mkdir(folder)


def get_consolidated_version(tag):
    """Analyze tag from a code source release, cast it to acceptable semver version X.X.X"""
    valid_version = True
    # Removing 'v' or 'V' from tag
    version_items = str(tag).replace('v', '').replace('V', '').split('.')
    # Add a patch number if only 2 items
    if len(version_items) == 2:
        version_items.append('0')
    # If more than 3, do not accept tag
    if len(version_items) > 3:
        valid_version = False
    # Verify if all items are digits
    for v in version_items:
        if not v.isdigit():
            valid_version = False
    # Return semver version and validity of it
    return '.'.join(version_items), valid_version


def manage_errors(repertoire_slug, version, reason):
    """Create dictionnary that will populate ERRORS_REPORT object"""
    errors = {}
    errors['schema'] = repertoire_slug
    errors['version'] = version
    errors['type'] = reason
    ERRORS_REPORT.append(errors)


def check_schema(repertoire_slug, conf, schema_type, folders):
    global SCHEMA_INFOS
    """Check validity of schema and all of its releases"""
    # schema_name in schema.data.gouv.fr is referenced by group and repo name in Git.
    # Ex : etalab / schema-irve-statique
    schema_name = '/'.join(conf['url'].split('.git')[0].split('/')[-2:])
    # define source folder and create it
    # source folder will help us to checkout to every release and analyze source code for each one
    src_folder = folders['CACHE_FOLDER'] + '/' + schema_name + '/'
    os.makedirs(src_folder, exist_ok=True)
    # clone repo in source folder
    Repo.clone_from(conf['url'], src_folder)
    repo = Repo(src_folder)
    # get tags of repo
    tags = sorted(repo.tags, key=lambda t: t.commit.committed_datetime)

    list_schemas = {}

    # Defining SCHEMA_INFOS object for website use
    SCHEMA_INFOS[schema_name] = {}
    SCHEMA_INFOS[schema_name]['homepage'] = conf['url']
    SCHEMA_INFOS[schema_name]['external_doc'] = conf.get('external_doc', None)
    SCHEMA_INFOS[schema_name]['external_tool'] = conf.get('external_tool', None)
    SCHEMA_INFOS[schema_name]['type'] = conf['type']
    SCHEMA_INFOS[schema_name]['email'] = conf['email']
    SCHEMA_INFOS[schema_name]['labels'] = conf.get('labels', None)
    SCHEMA_INFOS[schema_name]['consolidation_dataset_id'] = conf.get('consolidation', None)
    SCHEMA_INFOS[schema_name]['versions'] = {}

    # for every tag
    for t in tags:
        print("version ", t)
        # get semver version and validity of it
        version, valid_version = get_consolidated_version(t)
        # if semver ok
        if valid_version:
            # define destination folder and create it
            # destination folder will store pertinents files for website for each version of each schema
            dest_folder = folders['DATA_FOLDER1'] + '/' + schema_name + '/' + version + '/'
            os.makedirs(dest_folder, exist_ok=True)
            # checkout to current version
            g = Git(src_folder)
            g.checkout(str(t))

            conf_schema = None
            # Managing validation differently for each type of schema
            # tableschema will use frictionless package
            # jsonschema will use jsonschema package
            # other will only check if schema.yml file is present and contain correct information
            if schema_type == 'tableschema':
                list_schemas = manage_tableschema(
                    src_folder,
                    dest_folder,
                    list_schemas,
                    version,
                    schema_name,
                    repertoire_slug
                )
            if schema_type == 'jsonschema':
                list_schemas, conf_schema = manage_jsonschema(
                    src_folder,
                    dest_folder,
                    list_schemas,
                    version,
                    schema_name,
                    repertoire_slug
                )
            if schema_type == 'other':
                list_schemas, conf_schema = manage_other(
                    src_folder,
                    dest_folder,
                    list_schemas,
                    version,
                    schema_name,
                    repertoire_slug
                )

    # Find latest valid version and create a specific folder 'latest' copying files in it (for website use)
    latest_folder, sf = manage_latest_folder(schema_name, folders)
    # Indicate in schema_info object name of latest schema
    SCHEMA_INFOS[schema_name]['latest'] = sf
    schema_file = list_schemas[sf]
    # Complete catalog with all relevant information of schema in it
    print('conf: ', conf)
    print('conf_schema: ', conf_schema)
    schema_to_add_to_catalog = generate_catalog_object(
        latest_folder,
        list_schemas,
        schema_file,
        schema_type,
        schema_name,
        folders,
        conf_schema if conf_schema else conf
    )
    return schema_to_add_to_catalog


def check_datapackage(repertoire_slug, conf, folders):
    global SCHEMA_INFOS
    """Check validity of schemas from a datapackage repo for all of its releases"""
    # schema_name in schema.data.gouv.fr is referenced by group and repo name in Git.
    # Ex : etalab / schema-irve-statique

    # define source folder and create it
    # source folder will help us to checkout to every release and analyze source code for each one
    dpkg_name = '/'.join(conf['url'].split('.git')[0].split('/')[-2:])
    src_folder = folders['CACHE_FOLDER'] + '/' + '/'.join(conf['url'].split('.git')[0].split('/')[-2:]) + '/'
    os.makedirs(src_folder, exist_ok=True)
    # clone repo in source folder
    Repo.clone_from(conf['url'], src_folder)
    repo = Repo(src_folder)
    # get tags of repo
    tags = sorted(repo.tags, key=lambda t: t.commit.committed_datetime)

    list_schemas = {}
    schemas_to_add_to_catalog = []

    # Defining SCHEMA_INFOS object for website use
    SCHEMA_INFOS[dpkg_name] = {}
    SCHEMA_INFOS[dpkg_name]['homepage'] = conf['url']
    SCHEMA_INFOS[dpkg_name]['external_doc'] = conf.get('external_doc', None)
    SCHEMA_INFOS[dpkg_name]['external_tool'] = conf.get('external_tool', None)
    SCHEMA_INFOS[dpkg_name]['type'] = conf['type']
    SCHEMA_INFOS[dpkg_name]['email'] = conf.get('email', None)
    SCHEMA_INFOS[dpkg_name]['labels'] = conf.get('labels', None)
    SCHEMA_INFOS[dpkg_name]['consolidation_dataset_id'] = conf.get('consolidation', None)
    SCHEMA_INFOS[dpkg_name]['versions'] = {}
    SCHEMA_INFOS[dpkg_name]['schemas'] = []

    for t in tags:
        print("version ", t)
        # get semver version and validity of it
        version, valid_version = get_consolidated_version(t)
        # if semver ok
        if valid_version:
            g = Git(src_folder)
            g.checkout(str(t))

            SCHEMA_INFOS[dpkg_name]['versions'][version] = {}
            SCHEMA_INFOS[dpkg_name]['versions'][version]['pages'] = []

            dest_folder = folders['DATA_FOLDER1'] + '/' + dpkg_name + '/' + version + '/'
            os.makedirs(dest_folder, exist_ok=True)
            for f in ['README.md', 'SEE_ALSO.md', 'CHANGELOG.md', 'CONTEXT.md', 'datapackage.json']:
                if os.path.isfile(src_folder + f):
                    shutil.copyfile(src_folder + f, dest_folder + f)
                    if f != 'datapackage.json':
                        SCHEMA_INFOS[dpkg_name]['versions'][version]['pages'].append(f)
                    else:
                        SCHEMA_INFOS[dpkg_name]['versions'][version]['schema_url'] = '/' + dpkg_name + '/' + version + '/datapackage.json'

            # Verify that a file datapackage.json is present
            if os.path.isfile(src_folder + 'datapackage.json'):
                # Validate it with frictionless package
                frictionless_report = frictionless.validate(src_folder + 'datapackage.json')
                # If datapackage release is valid, then
                if frictionless_report['valid']:
                    with open(src_folder + 'datapackage.json') as out:
                        dp = json.load(out)

                    schemas_dp = [r['schema'] for r in dp['resources'] if 'schema' in r]

                    for schema in schemas_dp:

                        with open(src_folder + schema) as out:
                            schema_json = json.load(out)

                        schema_name = dpkg_name.split('/')[0] + '/' + schema_json['name']

                        if schema_name not in list_schemas:
                            list_schemas[schema_name] = {}

                        list_schemas[schema_name][version] = schema.split('/')[-1]

                        if schema_name not in SCHEMA_INFOS:
                            # Defining SCHEMA_INFOS object for website use
                            SCHEMA_INFOS[schema_name] = {}
                            SCHEMA_INFOS[schema_name]['homepage'] = conf['url']
                            SCHEMA_INFOS[schema_name]['external_doc'] = conf.get('external_doc', None)
                            SCHEMA_INFOS[schema_name]['external_tool'] = conf.get('external_tool', None)
                            SCHEMA_INFOS[schema_name]['type'] = 'tableschema'
                            SCHEMA_INFOS[schema_name]['email'] = conf.get('email', None)
                            SCHEMA_INFOS[schema_name]['versions'] = {}
                            SCHEMA_INFOS[schema_name]['datapackage'] = dp['title']
                            SCHEMA_INFOS[schema_name]['datapackage_id'] = dp['name']
                            SCHEMA_INFOS[schema_name]['labels'] = conf.get('labels', None)
                            SCHEMA_INFOS[schema_name]['consolidation_dataset_id'] = conf.get(
                                'consolidation',
                                None
                            )

                        SCHEMA_INFOS[schema_name]['versions'][version] = {}
                        SCHEMA_INFOS[schema_name]['versions'][version]['pages'] = []

                        # define destination folder and create it
                        # destination folder will store pertinents files for website
                        # for each version of each schema

                        schema_dest_folder = folders['DATA_FOLDER1'] + '/' + schema_name + '/' + version + '/'
                        if len(schema.split('/')) > 1:
                            os.makedirs(schema_dest_folder, exist_ok=True)
                        shutil.copyfile(src_folder + schema, schema_dest_folder + schema.split('/')[-1])
                        SCHEMA_INFOS[schema_name]['versions'][version]['schema_url'] = '/' + schema_name + '/' + version + '/' + schema.split("/")[-1]
                        for f in ['README.md', 'SEE_ALSO.md', 'CHANGELOG.md', 'CONTEXT.md']:
                            if os.path.isfile(src_folder + '/'.join(schema.split('/')[:-1]) + '/' + f):
                                shutil.copyfile(
                                    src_folder + '/'.join(schema.split('/')[:-1]) + '/' + f,
                                    schema_dest_folder + f
                                )
                                SCHEMA_INFOS[schema_name]['versions'][version]['pages'].append(f)

                        # Create documentation file and save it
                        with open(schema_dest_folder + '/' + 'documentation.md', "w") as out:
                            try:
                                # From schema.json, we use tableschema_to_markdown package to convert it in a
                                # readable mardown file that will be use for documentation
                                convert_source(schema_dest_folder + schema.split('/')[-1], out, 'page', [])
                                SCHEMA_INFOS[schema_name]['versions'][version]['pages'].append(
                                    'documentation.md'
                                )
                            except:
                                # if conversion is on error, we add it to ERRORS_REPORT
                                manage_errors(repertoire_slug, version, 'convert to markdown')

                        latest_folder, sf = manage_latest_folder(schema_name, folders)
                else:
                    print('not valid')
            else:
                print('no datapackage')
    # Find latest valid version and create a specific folder 'latest' copying files in it (for website use)
    latest_folder, sf = manage_latest_folder(dpkg_name, folders)

    for schema in schemas_dp:
        with open(src_folder + schema) as out:
            schema_json = json.load(out)
        # Complete catalog with all relevant information of schema in it
        statc = generate_catalog_object(
            folders['DATA_FOLDER1'] + '/' + dpkg_name.split('/')[0] + '/' + schema_json['name'] + '/latest/',
            list_schemas[dpkg_name.split('/')[0] + '/' + schema_json['name']],
            schema.split('/')[-1],
            'tableschema',
            dpkg_name,
            folders,
            conf,
            dp
        )
        schemas_to_add_to_catalog.append(statc)
        SCHEMA_INFOS[dpkg_name.split('/')[0] + '/' + schema_json['name']]['latest'] = sf
        SCHEMA_INFOS[dpkg_name]['schemas'].append(dpkg_name.split('/')[0] + '/' + schema_json['name'])

    SCHEMA_INFOS[dpkg_name]['latest'] = sf

    statc = generate_catalog_datapackage(
        latest_folder, dpkg_name,
        conf,
        list_schemas[dpkg_name.split('/')[0] + '/' + schema_json['name']]
    )
    schemas_to_add_to_catalog.append(statc)

    # schemas_to_add_to_catalog.append(schema_to_add_to_catalog)
    return schemas_to_add_to_catalog


def manage_tableschema(
    src_folder,
    dest_folder,
    list_schemas,
    version,
    schema_name,
    repertoire_slug,
    schema_file='schema.json'
):
    """Check validity of a schema release from tableschema type"""
    # Verify that a file schema.json is present
    if os.path.isfile(src_folder + schema_file):
        # Validate it with frictionless package
        frictionless_report = frictionless.validate_schema(src_folder + schema_file)
        # If schema release is valid, then
        if frictionless_report['valid']:
            list_schemas[version] = schema_file
            # We complete info of version
            SCHEMA_INFOS[schema_name]['versions'][version] = {}
            SCHEMA_INFOS[schema_name]['versions'][version]['pages'] = []
            subfolder = '/'.join(schema_file.split('/')[:-1]) + '/'
            if subfolder == '/':
                subfolder = ''
            else:
                os.makedirs(dest_folder + subfolder, exist_ok=True)
            # We check for list of normalized files if it is present in source code
            # if so, we copy paste them into dest folder
            for f in [schema_file, 'README.md', 'SEE_ALSO.md', 'CHANGELOG.md', 'CONTEXT.md']:
                if os.path.isfile(src_folder + subfolder + f):
                    shutil.copyfile(src_folder + subfolder + f, dest_folder + f)
                    # if it is a markdown file, we will read them as page in website
                    if f[-3:] == '.md':
                        SCHEMA_INFOS[schema_name]['versions'][version]['pages'].append(f)
                    # if it is the schema, we indicate it as it in object
                    if f == schema_file:
                        SCHEMA_INFOS[schema_name]['versions'][version]['schema_url'] = '/' + schema_name + '/' + version + '/' + schema_file
            # Create documentation file and save it
            with open(dest_folder + 'documentation.md', "w") as out:
                try:
                    # From schema.json, we use tableschema_to_markdown package to convert it in a
                    # readable mardown file that will be use for documentation
                    convert_source(dest_folder + schema_file, out, 'page', [])
                    SCHEMA_INFOS[schema_name]['versions'][version]['pages'].append('documentation.md')
                except:
                    # if conversion is on error, we add it to ERRORS_REPORT
                    manage_errors(repertoire_slug, version, 'convert to markdown')
        # If schema release is not valid, we remove it from DATA_FOLDER1
        else:
            manage_errors(repertoire_slug, version, 'tableschema validation')
            shutil.rmtree(dest_folder)
    # If there is no schema.json, schema release is not valid, we remove it from DATA_FOLDER1
    else:
        manage_errors(repertoire_slug, version, 'missing ' + schema_file)
        shutil.rmtree(dest_folder)

    return list_schemas


def manage_jsonschema(
    src_folder,
    dest_folder,
    list_schemas,
    version,
    schema_name,
    repertoire_slug
):
    """Check validity of a schema release from jsonschema type"""
    conf_schema = None
    # Verify that a file schemas.yml is present
    # This file will indicate title, description of jsonschema
    # (it is a prerequisite asked by schema.data.gouv.fr)
    # This file will also indicate which file store the jsonschema schema
    if os.path.isfile(src_folder + 'schemas.yml'):
        try:
            with open(src_folder + 'schemas.yml', "r") as f:
                conf_schema = yaml.safe_load(f)
                if 'schemas' in conf_schema:
                    s = conf_schema['schemas'][0]
                    # Verify if jsonschema file indicate in schemas.yml is present, then load it
                    if os.path.isfile(src_folder + s['path']):
                        with open(src_folder + s['path'], "r") as f:
                            schema_data = json.load(f)
                        # Validate schema with jsonschema package
                        jsonschema.validators.validator_for(schema_data).check_schema(schema_data)
                        list_schemas[version] = s['path']
                        # We complete info of version
                        SCHEMA_INFOS[schema_name]['versions'][version] = {}
                        SCHEMA_INFOS[schema_name]['versions'][version]['pages'] = []
                        # We check for list of normalized files if it is present in source code
                        # if so, we copy paste them into dest folder
                        for f in ['README.md', 'SEE_ALSO.md', 'CHANGELOG.md', 'CONTEXT.md', s['path']]:
                            if os.path.isfile(src_folder + f):
                                os.makedirs(os.path.dirname(dest_folder + f), exist_ok=True)
                                shutil.copyfile(src_folder + f, dest_folder + f)
                            # if it is a markdown file, we will read them as page in website
                            if f[-3:] == '.md':
                                SCHEMA_INFOS[schema_name]['versions'][version]['pages'].append(f)
                            # if it is the schema, we indicate it as it in object
                            if f == s['path']:
                                SCHEMA_INFOS[schema_name]['versions'][version]['schema_url'] = '/' + schema_name + '/' + version + '/' + s['path']
        # If schema release is not valid, we remove it from DATA_FOLDER1
        except:
            manage_errors(repertoire_slug, version, 'jsonschema validation')
            shutil.rmtree(dest_folder)
    # If there is no schemas.yml, schema release is not valid, we remove it from DATA_FOLDER1
    else:
        manage_errors(repertoire_slug, version, 'missing schemas.yml')
        shutil.rmtree(dest_folder)

    return list_schemas, conf_schema


def manage_other(
    src_folder,
    dest_folder,
    list_schemas,
    version,
    schema_name,
    repertoire_slug
):
    """Check validity of a schema release from other type"""
    conf_schema = None
    # Verify that a file schema.yml is present
    # This file will indicate title, description of schema
    # (it is a prerequisite asked by schema.data.gouv.fr)
    if os.path.isfile(src_folder + 'schema.yml'):
        try:
            with open(src_folder + 'schema.yml', "r") as f:
                conf_schema = yaml.safe_load(f)
            list_schemas[version] = 'schema.yml'
            # We complete info of version
            SCHEMA_INFOS[schema_name]['versions'][version] = {}
            SCHEMA_INFOS[schema_name]['versions'][version]['pages'] = []
            # We check for list of normalized files if it is present in source code
            # if so, we copy paste them into dest folder
            for f in ['README.md', 'SEE_ALSO.md', 'CHANGELOG.md', 'CONTEXT.md', 'schema.yml']:
                if os.path.isfile(src_folder + f):
                    shutil.copyfile(src_folder + f, dest_folder + f)
                    # if it is a markdown file, we will read them as page in website
                    if f[-3:] == '.md':
                        SCHEMA_INFOS[schema_name]['versions'][version]['pages'].append(f)
                    # if it is the schema, we indicate it as it in object
                    if f == 'schema.yml':
                        SCHEMA_INFOS[schema_name]['versions'][version]['schema_url'] = '/' + schema_name + '/' + version + '/' + 'schema.yml'
        # If schema release is not valid, we remove it from DATA_FOLDER1
        except:
            manage_errors(repertoire_slug, version, 'validation of type other')
            shutil.rmtree(dest_folder)
    # If there is no schema.yml, schema release is not valid, we remove it from DATA_FOLDER1
    else:
        manage_errors(repertoire_slug, version, 'missing schema.yml')
        shutil.rmtree(dest_folder)

    return list_schemas, conf_schema


def comparer_versions(version):
    return [int(part) if part.isnumeric() else np.inf for part in version.split('.')]


def manage_latest_folder(schema_name, folders):
    """Create latest folder containing all files from latest valid version of a schema"""
    # Get all valid version from a schema by analyzing folders
    # then sort them to get latest valid version and related folder
    subfolders = [f.name for f in os.scandir(folders['DATA_FOLDER1'] + '/' + schema_name + '/') if f.is_dir()]
    subfolders = sorted(subfolders, key=comparer_versions)
    sf = subfolders[-1]
    if sf == 'latest':
        sf = subfolders[-2]
    latest_version_folder = folders['DATA_FOLDER1'] + '/' + schema_name + '/' + sf + '/'
    # Determine latest folder path then mkdir it
    latest_folder = folders['DATA_FOLDER1'] + '/' + schema_name + '/latest/'
    os.makedirs(latest_folder, exist_ok=True)
    # For every file in latest valid version folder, copy them into
    shutil.copytree(latest_version_folder, latest_folder, dirs_exist_ok=True)
    # For website need, copy paste latest README into root of schema folder
    shutil.copyfile(
        latest_version_folder + 'README.md',
        '/'.join(latest_version_folder.split('/')[:-2]) + '/README.md'
    )

    return latest_folder, sf


def generate_catalog_datapackage(latest_folder, dpkg_name, conf, list_schemas):
    with open(latest_folder + 'datapackage.json', "r") as f:
        dpkg = json.load(f)
    mydict = {}
    mydict['name'] = dpkg_name
    mydict['title'] = dpkg['title']
    mydict['description'] = dpkg['description']
    mydict['schema_url'] = 'https://schema.data.gouv.fr/schemas/' + dpkg_name + '/latest/' + 'datapackage.json'
    mydict['schema_type'] = 'datapackage'
    mydict['contact'] = conf.get('email', None)
    mydict['examples'] = []
    mydict['labels'] = conf['labels'] if 'labels' in conf else []
    mydict['consolidation_dataset_id'] = conf.get('consolidation', None)
    mydict['versions'] = []
    for sf in list_schemas:
        mydict2 = {}
        mydict2['version_name'] = sf
        mydict2['schema_url'] = 'https://schema.data.gouv.fr/schemas/' + dpkg_name + '/' + sf + '/' + 'datapackage.json'
        mydict['versions'].append(mydict2)
    # These four following property are not in catalog spec
    mydict['external_doc'] = conf.get('external_doc', None)
    mydict['external_tool'] = conf.get('external_tool', None)
    mydict['homepage'] = conf['url']
    return mydict


def generate_catalog_object(
    latest_folder,
    list_schemas,
    schema_file,
    schema_type,
    schema_name,
    folders,
    obj_info=None,
    datapackage=None,
):
    """Generate dictionnary containing all relevant information for catalog"""
    # If tableschema, relevant information are directly into schema.json,
    # if not, relevant info are in yaml files with are stored in obj_info variable
    if schema_type == 'tableschema':
        with open(latest_folder + schema_file, "r") as f:
            schema = json.load(f)
    else:
        schema = obj_info
    # Complete dictionnary with relevant info needed in catalog
    mydict = {}
    if datapackage:
        mydict['name'] = latest_folder.replace(folders['DATA_FOLDER1'] + '/', '').replace('/latest/', '')
    else:
        mydict['name'] = schema_name
    mydict['title'] = schema['title']
    mydict['description'] = schema['description']
    mydict['schema_url'] = 'https://schema.data.gouv.fr/schemas/' + mydict['name'] + '/latest/' + schema_file
    mydict['schema_type'] = schema_type
    mydict['contact'] = obj_info.get('email', None)
    mydict['examples'] = schema.get('resources', [])
    mydict['labels'] = obj_info.get('labels', [])
    mydict['consolidation_dataset_id'] = obj_info.get('consolidation', None)
    mydict['versions'] = []
    for sf in list_schemas:
        mydict2 = {}
        mydict2['version_name'] = sf
        mydict2['schema_url'] = 'https://schema.data.gouv.fr/schemas/' + mydict['name'] + '/' + sf + '/' + list_schemas[sf]
        mydict['versions'].append(mydict2)
    # These four following property are not in catalog spec
    mydict['external_doc'] = obj_info.get('external_doc', None)
    mydict['external_tool'] = obj_info.get('external_tool', None)
    mydict['homepage'] = obj_info.get('homepage', obj_info.get('url', None))
    if datapackage:
        mydict['datapackage_title'] = datapackage['title']
        mydict['datapackage_name'] = schema_name
        mydict['datapackage_description'] = datapackage.get('description', None)
    return mydict


def find_md_links(md):
    """Returns dict of links in markdown:
    'regular': [foo](some.url)
    'footnotes': [foo][3]
    [3]: some.url
    """
    # https://stackoverflow.com/a/30738268/2755116
    INLINE_LINK_RE = re.compile(r'\[([^\]]+)\]\(([^)]+)\)')
    FOOTNOTE_LINK_TEXT_RE = re.compile(r'\[([^\]]+)\]\[(\d+)\]')
    FOOTNOTE_LINK_URL_RE = re.compile(r'\[(\d+)\]:\s+(\S+)')

    links = list(INLINE_LINK_RE.findall(md))
    footnote_links = dict(FOOTNOTE_LINK_TEXT_RE.findall(md))
    footnote_urls = dict(FOOTNOTE_LINK_URL_RE.findall(md))

    footnotes_linking = []

    for key in footnote_links.keys():
        footnotes_linking.append((footnote_links[key], footnote_urls[footnote_links[key]]))

    return links


def cleanLinksDocumentation(dest_folder):
    """Custom cleaning for links in markdown"""
    # For every documentation.md file, do some custom cleaning for links
    file = codecs.open(dest_folder + 'documentation.md', "r", "utf-8")
    data = file.read()
    file.close()
    # Find all links in file
    links = find_md_links(data)
    # For each one, lower string then manage space ; _ ; --- and replace them by -
    for (name, link) in links:
        if link.startswith('#'):
            newlink = link.lower()
            newlink = newlink.replace(' ', '-')
            newlink = newlink.replace('_', '-')
            newlink = unidecode(newlink, "utf-8")
            newlink = newlink.replace('---', '-')
            data = data.replace(link, newlink)
    # Save modifications
    with open(dest_folder + 'documentation.md', 'w', encoding='utf-8') as fin:
        fin.write(data)


def addFrontToMarkdown(dest_folder, f):
    """Custom add to every markdown files"""
    # for every markdown files
    file = codecs.open(dest_folder + f, "r", "utf-8")
    data = file.read()
    file.close()
    # Add specific tag for website interpretation
    data = "<MenuSchema />\n\n" + data
    # Exception scdl Budget not well interpreted by vuepress
    data = data.replace('<DocumentBudgetaire>', 'DocumentBudgetaire')
    # Save modification
    with open(dest_folder + f, 'w', encoding='utf-8') as fin:
        fin.write(data)


def getListOfFiles(dirName):
    """Get list off all files in a specific folder"""
    # create a list of file and sub directories
    # names in the given directory
    listOfFile = os.listdir(dirName)
    allFiles = list()
    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory
        if os.path.isdir(fullPath):
            allFiles = allFiles + getListOfFiles(fullPath)
        else:
            allFiles.append(fullPath)
    return allFiles


def get_contributors(url):
    """Get list off all contributors of a specific git repo"""
    parse_url = parse.urlsplit(url)
    # if github, use github api
    if 'github.com' in parse_url.netloc:
        api_url = parse_url.scheme + '://api.github.com/repos/' + parse_url.path[1:].replace('.git', '') + '/contributors'
    # else, use gitlab api
    else:
        api_url = parse_url.scheme + '://' + parse_url.netloc + '/api/v4/projects/' + parse_url.path[1:].replace('/', '%2F').replace('.git', '') + '/repository/contributors'
    try:
        r = requests.get(api_url)
        return len(r.json())
    except:
        return None


################################################################################################################
# DAG functions

def check_and_save_schemas(ti):
    folders = ti.xcom_pull(key='folders', task_ids='initialization')
    config = ti.xcom_pull(key='config', task_ids='initialization')
    # Clean and (re)create CACHE AND DATA FOLDER
    clean_and_create_folder(folders['CACHE_FOLDER'])
    clean_and_create_folder(folders['DATA_FOLDER1'])

    # Initiate Catalog
    SCHEMA_CATALOG['$schema'] = 'https://opendataschema.frama.io/catalog/schema-catalog.json'
    SCHEMA_CATALOG['version'] = 1
    SCHEMA_CATALOG['schemas'] = []

    # For every schema in repertoires.yml, check it
    for repertoire_slug, conf in config.items():
        print("\n\nStarting with ", repertoire_slug)
        print(conf)
        try:
            if conf['type'] != 'datapackage':
                print('Recognized as a simple schema')
                schema_to_add_to_catalog = check_schema(repertoire_slug, conf, conf['type'], folders)
                SCHEMA_CATALOG['schemas'].append(schema_to_add_to_catalog)
            else:
                print('Recognized as a datapackage')
                schemas_to_add_to_catalog = check_datapackage(repertoire_slug, conf, folders)
                for schema in schemas_to_add_to_catalog:
                    SCHEMA_CATALOG['schemas'].append(schema)
            # Append info to SCHEMA_CATALOG
            print(f'--- {repertoire_slug} processed')
        except git.exc.GitCommandError:
            print(f'--- {repertoire_slug} failed to process due to git error')

    schemas_scdl = SCHEMA_CATALOG.copy()
    schemas_transport = SCHEMA_CATALOG.copy()
    schemas_tableschema = SCHEMA_CATALOG.copy()

    # Save catalog to schemas.json file
    with open(folders['DATA_FOLDER1'] + '/schemas.json', 'w') as fp:
        json.dump(SCHEMA_CATALOG, fp)

    schemas_scdl['schemas'] = [
        x for x in schemas_scdl['schemas'] if 'Socle Commun des Données Locales' in x['labels']
    ]

    schemas_transport['schemas'] = [
        x for x in schemas_transport['schemas'] if 'transport.data.gouv.fr' in x['labels']
    ]

    schemas_tableschema['schemas'] = [
        x for x in schemas_tableschema['schemas'] if x['schema_type'] == 'tableschema'
    ]

    with open(folders['DATA_FOLDER1'] + '/schemas-scdl.json', 'w') as fp:
        json.dump(schemas_scdl, fp)

    with open(folders['DATA_FOLDER1'] + '/schemas-transport-data-gouv-fr.json', 'w') as fp:
        json.dump(schemas_transport, fp)

    with open(folders['DATA_FOLDER1'] + '/schemas-tableschema.json', 'w') as fp:
        json.dump(schemas_tableschema, fp)

    # Save schemas_infos to schema-infos.json file
    with open(folders['DATA_FOLDER1'] + '/schema-infos.json', 'w') as fp:
        json.dump(SCHEMA_INFOS, fp)

    # Save errors to errors.json file
    with open(folders['DATA_FOLDER1'] + '/errors.json', 'w') as fp:
        json.dump(ERRORS_REPORT, fp)

    ti.xcom_push(key='SCHEMA_CATALOG', value=SCHEMA_CATALOG)
    ti.xcom_push(key='SCHEMA_INFOS', value=SCHEMA_INFOS)
    ti.xcom_push(key='ERRORS_REPORT', value=ERRORS_REPORT)
    print('End of process catalog: ', SCHEMA_CATALOG)
    print('End of process infos: ', SCHEMA_INFOS)
    print('End of process errors: ', ERRORS_REPORT)


def sort_folders(ti):
    SCHEMA_CATALOG = ti.xcom_pull(key='SCHEMA_CATALOG', task_ids='check_and_save_schemas')
    folders = ti.xcom_pull(key='folders', task_ids='initialization')
    # Get list of all files in DATA_FOLDER
    files = []
    files = getListOfFiles(folders['DATA_FOLDER1'])
    # Create list of file that we do not want to copy paste
    avoid_files = [folders['DATA_FOLDER1'] + '/' + s['name'] + '/README.md' for s in SCHEMA_CATALOG['schemas']]
    # for every file
    for f in files:
        # if it is a markdown, add custom front to content
        if f[-3:] == '.md':
            addFrontToMarkdown('/'.join(f.split('/')[:-1]) + '/', f.split('/')[-1])
        # if it is the documentation file, clean links on it
        if f.split('/')[-1] == 'documentation.md':
            cleanLinksDocumentation('/'.join(f.split('/')[:-1]) + '/')
        # if it is a README file (except if on avoid_list)
        # then copy paste it to root folder of schema (for website use)
        # That will create README file with name X.X.X.md (X.X.X corresponding to a specific version)
        if f.split('/')[-1] == 'README.md':
            if f not in avoid_files:
                shutil.copyfile(f, f.replace('/README.md', '.md'))

    # Clean and (re)create DATA_FOLDER2, then copy paste all DATA_FOLDER1 into DATA_FOLDER2
    # DATA_FOLDER1 will be use to contain all markdown files
    # DATA_FOLDER2 will be use to contain all yaml and json files
    # This is needed for vuepress that need to store page in one place and 'resources' in another
    if os.path.exists(folders['DATA_FOLDER2']):
        shutil.rmtree(folders['DATA_FOLDER2'])
    shutil.copytree(folders['DATA_FOLDER1'], folders['DATA_FOLDER2'])


def get_issues_and_labels(ti):
    folders = ti.xcom_pull(key='folders', task_ids='initialization')
    # For every issue, request them by label schema status (en investigation or en construction)
    mydict = {}
    labels = ['construction', 'investigation']
    # For each label, get relevant info via github api of schema.data.gouv.fr repo
    try:
        for lab in labels:
            r = requests.get(
                'https://api.github.com/repos/etalab/schema.data.gouv.fr/issues?q=is%3Aopen+is%3Aissue&labels=Sch%C3%A9ma%20en%20'
                + lab
            )
            mydict[lab] = []
            for issue in r.json():
                mydict2 = {}
                mydict2['created_at'] = issue['created_at']
                mydict2['labels'] = [lab]
                mydict2['nb_comments'] = issue['comments']
                mydict2['title'] = issue['title']
                mydict2['url'] = issue['html_url']
                mydict[lab].append(mydict2)

        # Find number of current issue in schema.data.gouv.fr repo
        r = requests.get('https://api.github.com/repos/etalab/schema.data.gouv.fr/issues?q=is%3Aopen+is%3Aissue')
        mydict['nb_issues'] = len(r.json())

        # for every schema, find relevant info in data.gouv.fr API
        mydict['references'] = {}
        for s in SCHEMA_CATALOG['schemas']:
            r = requests.get('https://www.data.gouv.fr/api/1/datasets/?schema=' + s['name'])
            mydict['references'][s['name']] = {}
            mydict['references'][s['name']]['dgv_resources'] = r.json()['total']
            mydict['references'][s['name']]['title'] = s['title']
            mydict['references'][s['name']]['contributors'] = get_contributors(s['homepage'])

        # Save stats infos to stats.json file
        with open(folders['DATA_FOLDER2'] + '/stats.json', 'w') as fp:
            json.dump(mydict, fp)
    except:
        pass


def remove_all_files_extension(folder, extension):
    """Remove all file of a specific extension in a folder"""
    files = []
    files = getListOfFiles(folder)
    for f in files:
        if f[-1 * len(extension):] == extension:
            os.remove(f)


def final_clean_up(ti):
    folders = ti.xcom_pull(key='folders', task_ids='initialization')
    # Remove all markdown from DATA_FOLDER1 and all json, yaml and yml file of DATA_FOLDER2
    remove_all_files_extension(folders['DATA_FOLDER2'], '.md')
    remove_all_files_extension(folders['DATA_FOLDER1'], '.json')
    remove_all_files_extension(folders['DATA_FOLDER1'], '.yml')
    remove_all_files_extension(folders['DATA_FOLDER1'], '.yaml')
