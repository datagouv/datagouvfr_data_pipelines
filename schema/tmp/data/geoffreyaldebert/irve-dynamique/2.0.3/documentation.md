## irve-dynamique

Infrastructures de recharge pour véhicules électriques

Spécification du fichier d'échange relatif aux données concernant la localisation géographique et les caractéristiques techniques des stations et des points de recharge pour véhicules électriques

- Schéma créé le : 06/29/18
- Site web : https://github.com/etalab/schema-irve
- Version : 2.0.4

### Modèle de données


##### Liste des propriétés

| Propriété | Type | Obligatoire |
| -- | -- | -- |
| [nom_amenageur](#propriété-nom_amenageur) | chaîne de caractères  | Non |
| [siren_amenageur](#propriété-siren_amenageur) | chaîne de caractères  | Oui |

#### Propriété `nom_amenageur`

> *Description : La dénomination sociale du nom de l'aménageur, c'est à dire de l'entité publique ou privée propriétaire des infrastructures. Vous pouvez accéder à cette dénomination exacte sur le site annuaire-entreprises.data.gouv.fr. Ce champs n'est pas obligatoire car il sera automatiquement renseigné lors de la constitution du fichier global de consolidation des IRVE.<br/>Ex : Société X, Entité Y*
- Valeur optionnelle
- Type : chaîne de caractères

#### Propriété `siren_amenageur`

> *Description : Le numero SIREN de l'aménageur issue de la base SIRENE des entreprises. Vous pouvez récupérer cet identifiant sur le site annuaire-entreprises.data.gouv.fr.<br/>Ex : 130025265*
- Valeur obligatoire
- Type : chaîne de caractères
- Motif : `^\d{9}$`
