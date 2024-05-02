import numpy as np

translate_types = {
    "string": "chaîne de caractères",
    "integer": "nombre entier",
    "number": "nombre",
    "boolean": "booléen",
    "object": "dictionnaire (clés-valeurs)",
    "array": "liste",
    "null": "vide",
}


def accordion(action, indent, indent_char):
    if action == "open":
        return f"\n{indent*indent_char}<blockquote>\n{indent*indent_char}<details>\n"
    elif action == "close":
        return f"\n{indent*indent_char}</details>\n{indent*indent_char}</blockquote>\n"


def jsonschema_to_markdown(jsonschema, indent=0, parent='', indent_char=''):
    # we'll add introduction from other metadata (name, title, description, website, version, jsonschema)
    md = ""
    for prop in jsonschema['properties'].keys():
        processed = False
#         print("Inspecting:", parent+'.'+prop)
        spec = jsonschema['properties'][prop]
#         print(spec)
        if '$ref' in spec:
            ref = spec['$ref'].split('/')
            # internal references are expected like "#/definitions/path/to/ref"
            # but sometimes they are just "path/to/ref"
            idx = np.argwhere([k == "definitions" for k in ref])
            if len(idx):
                ref = ref[idx[0][0]:]
            else:
                ref = ["definitions"] + ref
            inner = jsonschema
            for k in ref:
                inner = inner[k]
            md += jsonschema_to_markdown(
                {'properties': {prop: inner}, 'definitions': jsonschema['definitions']},
                indent=indent, parent=parent + '.' + prop
            )
        elif 'title' in spec or 'description' in spec:
            title = spec.get('title', prop)
            md += indent * indent_char + f"#### {title} - Propriété {prop}\n"
            if spec.get('description'):
                md += indent * indent_char + f"> *Description : {spec['description'].strip()}*<br>\n"
            if spec.get('exemple') or spec.get('examples'):
                example = spec.get('exemple') or spec.get('examples')
                if isinstance(example, list):
                    example = example[0]
                md += indent * indent_char + f"> *Exemple : {example}*\n"
            if prop in jsonschema.get('required', []):
                md += indent * indent_char + "- Valeur obligatoire\n"
            else:
                md += indent * indent_char + "- Valeur optionnelle\n"
            if 'type' in spec:
                if isinstance(spec['type'], list):
                    type_ = " ou ".join([
                        translate_types.get(_, _) for _ in spec['type']
                    ])
                else:
                    type_ = translate_types.get(spec['type'], spec['type'])
                md += indent * indent_char + f"- Type : {type_}\n"
            if 'pattern' in spec:
                md += indent * indent_char + f"- Motif : `{spec['pattern']}`\n"
            if 'enum' in spec or spec.get('items', {}).get('enum'):
                md += f"{indent*indent_char}- Valeurs autorisées :\n"
                enum = spec.get('enum') or spec.get('items', {}).get('enum')
                for v in enum:
                    md += f"{indent*indent_char}   - {v}\n"
            processed = True
        if 'properties' in spec:
            if not processed:
                md += indent * indent_char + f"#### {prop} - Propriété {prop}\n"
                md += indent * indent_char + f"- Type : {translate_types['object']}\n"
            md += accordion("open", indent, indent_char)
            md += (
                f"\n{indent*indent_char}<summary>Cet objet doit contenir"
                " les champs suivants :</summary>\n\n"
            )
            md += jsonschema_to_markdown(
                {'properties': spec['properties'], 'definitions': jsonschema['definitions']},
                indent=indent + 1, parent=parent + '.' + prop
            )
            md += accordion("close", indent, indent_char)
        if 'items' in spec:
            if isinstance(spec['items'], dict):
                items = spec['items']
            elif isinstance(spec['items'], list):
                if len(spec['items']) > 1:
                    raise NotImplementedError("Mutliple items in list")
                items = spec['items'][0]
            if '$ref' in items or 'title' in items or 'description' in items:
                # if isUrl(spec['items']['$ref']): ...
                if '$ref' in items:
                    ref = items['$ref'].split('/')[1:]
                    inner = jsonschema
                    for k in ref:
                        inner = inner[k]
                else:
                    k = prop
                    inner = items
                if not processed:
                    md += indent * indent_char + f"#### {prop} - Propriété {prop}\n"
                    md += indent * indent_char + f"- Type : {translate_types.get('array')}\n"
                md += accordion("open", indent, indent_char)
                md += (
                    f"\n{indent*indent_char}<summary>Cette propriété doit contenir une liste"
                    " d'éléments parmi les suivants :</summary>\n\n"
                )
                md += jsonschema_to_markdown(
                    {'properties': {k: inner}, 'definitions': jsonschema.get('definitions')},
                    indent=indent + 1, parent=parent + '.' + prop
                )
                md += accordion("close", indent, indent_char)
            elif any(k in spec['items'] for k in ['anyOf', 'allOf']):
                if not processed:
                    md += indent * indent_char + f"#### {prop} - Propriété {prop}\n"
                    md += indent * indent_char + f"- Type : {translate_types.get('array')}\n"
                md += accordion("open", indent, indent_char)
                if 'anyOf' in spec['items']:
                    xOf = 'anyOf'
                    md += (
                        f"\n{indent*indent_char}<summary>Cette propriété doit contenir une"
                        " liste d'éléments parmi les suivants :</summary>\n\n"
                    )
                else:
                    xOf = 'allOf'
                    md += (
                        f"\n{indent*indent_char}<summary>Cette propriété doit contenir une"
                        " liste  avec tous les éléments suivants :</summary>\n\n"
                    )
                for option in spec['items'][xOf]:
                    ref = option['$ref'].split('/')[1:]
                    inner = jsonschema
                    for k in ref:
                        inner = inner[k]
                    md += jsonschema_to_markdown(
                        {'properties': {k: inner}, 'definitions': jsonschema['definitions']},
                        indent=indent + 1, parent=parent + '.' + prop
                    )
                md += accordion("close", indent, indent_char)
            elif 'properties' in items:
                md += jsonschema_to_markdown(
                    {'properties': items['properties'], 'definitions': jsonschema.get('definitions')},
                    indent=indent + 1, parent=parent + '.' + prop
                )
        if 'enum' in spec and not processed:
            md += indent * indent_char + f"#### {prop} - Propriété {prop}\n"
            md += indent * indent_char + f"- Type : {translate_types.get('array')}\n"
            md += f"{indent*indent_char}- Valeurs autorisées :\n"
            for v in spec.get('enum'):
                md += f"{indent*indent_char}   - {v}\n"
        md += '\n'
    return md.replace('\n' * 3, '\n' * 2)
