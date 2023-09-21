import yaml
from git import Repo, Git
import os
import shutil
from table_schema_to_markdown import convert_source
import frictionless
import glob
import json
import jsonschema
import re
from unidecode import unidecode
import codecs
import requests
from urllib import parse
import numpy as np
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)

msgs = "Ran from Airflow {{ ds }} !"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}{DAG_NAME}/"
OUTPUT_DATA_FOLDER = f"{TMP_FOLDER}output/"
DATE_AIRFLOW = "{{ ds }}"
LIST_SCHEMAS_YAML = "https://raw.githubusercontent.com/etalab/schema.data.gouv.fr/main/repertoires.yml"

CACHE_FOLDER = TMP_FOLDER + 'cache'
DATA_FOLDER1 = TMP_FOLDER + 'data'
DATA_FOLDER2 = TMP_FOLDER + 'data2'
ERRORS_REPORT = []
SCHEMA_INFOS = {}
SCHEMA_CATALOG = {}

# Loading yaml file containing all schemas that we want to display in schema.data.gouv.fr
r = requests.get(LIST_SCHEMAS_YAML)

with open(TMP_FOLDER + 'repertoires.yml', 'wb') as f:
    f.write(r.content)

with open(TMP_FOLDER + 'repertoires.yml', "r") as f:
    config = yaml.safe_load(f)


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


def check_schema(repertoire_slug, conf, schema_type):
    """Check validity of schema and all of its releases"""
    # schema_name in schema.data.gouv.fr is referenced by group and repo name in Git. Ex : etalab / schema-irve-statique
    schema_name = '/'.join(conf['url'].split('.git')[0].split('/')[-2:])
    # define source folder and create it
    # source folder will help us to checkout to every release and analyze source code for each one
    src_folder = CACHE_FOLDER + '/' + schema_name + '/'
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
    
    # for every tags
    for t in tags:
        # get semver version and validity of it
        version, valid_version = get_consolidated_version(t)
        # if semver ok
        if(valid_version):
            # define destination folder and create it
            # destination folder will store pertinents files for website for each version of each schema
            dest_folder = DATA_FOLDER1 + '/' + schema_name + '/' + version + '/'
            os.makedirs(dest_folder, exist_ok=True)
            # checkout to current version
            g = Git(src_folder)
            g.checkout(str(t))
            
            conf_schema = None
            # Managing validation differently for each type of schema
            # tableschema will use frictionless package
            # jsonschema will use jsonschema package
            # other will only check if schema.yml file is present and contain correct information
            if(schema_type == 'tableschema'):
                list_schemas = manage_tableschema(src_folder, dest_folder, list_schemas, version, schema_name)
            if(schema_type == 'jsonschema'):
                list_schemas, conf_schema = manage_jsonschema(src_folder, dest_folder, list_schemas, version, schema_name)
            if(schema_type == 'other'):
                list_schemas, conf_schema = manage_other(src_folder, dest_folder, list_schemas, version, schema_name)
          
    # Find latest valid version and create a specific folder 'latest' copying files in it (for website use)
    latest_folder, sf = manage_latest_folder(conf, schema_name)
    # Indicate in schema_info object name of latest schema
    SCHEMA_INFOS[schema_name]['latest'] = sf
    schema_file = list_schemas[sf]
    # Complete catalog with all relevant information of schema in it
    schema_to_add_to_catalog = generate_catalog_object(latest_folder, list_schemas, schema_file, schema_type, schema_name, conf_schema)
    return schema_to_add_to_catalog


def check_datapackage(repertoire_slug, conf, schema_type):
    """Check validity of schemas from a datapackage repo for all of its releases"""
    # schema_name in schema.data.gouv.fr is referenced by group and repo name in Git. Ex : etalab / schema-irve-statique
    
    # define source folder and create it
    # source folder will help us to checkout to every release and analyze source code for each one
    dpkg_name = '/'.join(conf['url'].split('.git')[0].split('/')[-2:])
    src_folder = CACHE_FOLDER + '/' + '/'.join(conf['url'].split('.git')[0].split('/')[-2:]) + '/'
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
        # get semver version and validity of it
        version, valid_version = get_consolidated_version(t)
        # if semver ok
        if(valid_version):
            g = Git(src_folder)
            g.checkout(str(t))

            SCHEMA_INFOS[dpkg_name]['versions'][version] = {}
            SCHEMA_INFOS[dpkg_name]['versions'][version]['pages'] = []
            
            dest_folder = DATA_FOLDER1 + '/' + dpkg_name + '/' + version + '/'
            os.makedirs(dest_folder, exist_ok=True)
            for f in ['README.md', 'SEE_ALSO.md', 'CHANGELOG.md', 'CONTEXT.md', 'datapackage.json']:
                if(os.path.isfile(src_folder + f)):
                    shutil.copyfile(src_folder + f, dest_folder + f)
                    if f != 'datapackage.json':
                        SCHEMA_INFOS[dpkg_name]['versions'][version]['pages'].append(f)
                    else:
                        SCHEMA_INFOS[dpkg_name]['versions'][version]['schema_url'] = '/' + dpkg_name + '/' + version + '/datapackage.json'

            # Verify that a file datapackage.json is present
            if(os.path.isfile(src_folder + 'datapackage.json')):
                # Validate it with frictionless package
                frictionless_report = frictionless.validate(src_folder + 'datapackage.json')
                # If datapackage release is valid, then
                if(frictionless_report['valid'] == True):
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
                            SCHEMA_INFOS[schema_name]['consolidation_dataset_id'] = conf.get('consolidation', None)
                        
                        SCHEMA_INFOS[schema_name]['versions'][version] = {}
                        SCHEMA_INFOS[schema_name]['versions'][version]['pages'] = []
                        
                        # define destination folder and create it
                        # destination folder will store pertinents files for website for each version of each schema
                    
                        schema_dest_folder = DATA_FOLDER1 + '/' + schema_name + '/' + version + '/'
                        if len(schema.split('/')) > 1:
                            os.makedirs(schema_dest_folder, exist_ok=True)
                        shutil.copyfile(src_folder + schema, schema_dest_folder + schema.split('/')[-1])
                        SCHEMA_INFOS[schema_name]['versions'][version]['schema_url'] = '/' + schema_name + '/' + version + '/' + schema.split("/")[-1]
                        for f in ['README.md', 'SEE_ALSO.md', 'CHANGELOG.md', 'CONTEXT.md']:
                            if(os.path.isfile(src_folder + '/'.join(schema.split('/')[:-1]) + '/' + f)):
                                shutil.copyfile(src_folder + '/'.join(schema.split('/')[:-1]) + '/' + f, schema_dest_folder + f)
                                SCHEMA_INFOS[schema_name]['versions'][version]['pages'].append(f)

                        # Create documentation file and save it
                        with open(schema_dest_folder + '/' + 'documentation.md', "w") as out:
                            try:
                                # From schema.json, we use tableschema_to_markdown package to convert it in a
                                # readable mardown file that will be use for documentation
                                convert_source(schema_dest_folder + schema.split('/')[-1], out, 'page',[])
                                SCHEMA_INFOS[schema_name]['versions'][version]['pages'].append('documentation.md')
                            except:
                                # if conversion is on error, we add it to ERRORS_REPORT
                                manage_errors(repertoire_slug, version, 'convert to markdown')
                        
                        latest_folder, sf = manage_latest_folder(conf, schema_name)      
                else:
                    print('not valid')
            else:
                print('no datapackage')
    # Find latest valid version and create a specific folder 'latest' copying files in it (for website use)
    latest_folder, sf = manage_latest_folder(conf, dpkg_name)

    for schema in schemas_dp:
        with open(src_folder + schema) as out:
            schema_json = json.load(out)
        # Complete catalog with all relevant information of schema in it
        statc = generate_catalog_object(
            DATA_FOLDER1 + '/' + dpkg_name.split('/')[0] + '/' + schema_json['name'] + '/latest/',
            list_schemas[dpkg_name.split('/')[0] + '/' + schema_json['name']],
            schema.split('/')[-1],
            'tableschema',
            dpkg_name,
            None,
            dp
        )
        schemas_to_add_to_catalog.append(statc)
        SCHEMA_INFOS[dpkg_name.split('/')[0] + '/' + schema_json['name']]['latest'] = sf
        SCHEMA_INFOS[dpkg_name]['schemas'].append(dpkg_name.split('/')[0] + '/' + schema_json['name'])

    SCHEMA_INFOS[dpkg_name]['latest'] = sf

    statc = generate_catalog_datapackage(latest_folder, dpkg_name, conf, list_schemas[dpkg_name.split('/')[0] + '/' + schema_json['name']])
    schemas_to_add_to_catalog.append(statc)
    
    # schemas_to_add_to_catalog.append(schema_to_add_to_catalog)
    return schemas_to_add_to_catalog


def manage_tableschema(src_folder, dest_folder, list_schemas, version, schema_name, schema_file='schema.json'):
    """Check validity of a schema release from tableschema type"""
    # Verify that a file schema.json is present
    if(os.path.isfile(src_folder + schema_file)):
        # Validate it with frictionless package
        frictionless_report = frictionless.validate_schema(src_folder + schema_file)
        # If schema release is valid, then        
        if(frictionless_report['valid'] == True):
            list_schemas[version] = schema_file
            # We complete info of version
            SCHEMA_INFOS[schema_name]['versions'][version] = {}
            SCHEMA_INFOS[schema_name]['versions'][version]['pages'] = []
            subfolder = '/'.join(schema_file.split('/')[:-1]) + '/'
            if subfolder == '/':
                subfolder = ''
            else:
                os.makedirs(dest_folder + subfolder, exist_ok=True)
            # We check for list of normalized files if it is present in source code, if so, we copy paste them into dest folder
            for f in [schema_file, 'README.md', 'SEE_ALSO.md', 'CHANGELOG.md', 'CONTEXT.md']:
                if(os.path.isfile(src_folder + subfolder + f)):
                    shutil.copyfile(src_folder + subfolder + f, dest_folder + f)
                    # if it is a markdown file, we will read them as page in website
                    if(f[-3:] == '.md'):
                        SCHEMA_INFOS[schema_name]['versions'][version]['pages'].append(f)
                    # if it is the schema, we indicate it as it in object
                    if(f == schema_file):
                        SCHEMA_INFOS[schema_name]['versions'][version]['schema_url'] = '/' + schema_name + '/' + version + '/' + schema_file
            # Create documentation file and save it
            with open(dest_folder + 'documentation.md', "w") as out:
                try:
                    # From schema.json, we use tableschema_to_markdown package to convert it in a
                    # readable mardown file that will be use for documentation
                    convert_source(dest_folder + schema_file, out, 'page',[])
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


