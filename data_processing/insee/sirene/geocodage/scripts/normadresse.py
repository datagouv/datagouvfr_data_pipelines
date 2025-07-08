#!/usr/bin/env python3

import re
import csv
from unidecode import unidecode
import sys
import select

# chargement des expressions régulières de traitement
regles = []
with open('resources/normadresse.csv') as abbrev_csv:
    for row in csv.DictReader(abbrev_csv):
        row['etape'] = float(row['etape'])
        regles.append(row)

debug = False


def abrev_out(orig, lib, max_out):
    "sélectionne les mots courts dans l'ordre de gauche à droite"
    court = lib.split()
    long = orig.split()
    # élimination des résidus de mots multiples abrégé, ex: ROND POINT > RPT
    for m in range(len(court)-1, 0, -1):
        if court[m] == '@':
            long[m-1] = court[m-1]
            del long[m]
            del court[m]
            if debug:
                print(long, court)

    for m in range(1, len(court)):
        out = (" ".join(court[0:m])+" "+" ".join(long[m:])).strip()
        if (len(out) <= max_out):
            return(out, True, lib.replace(' @', ''))
    return(out, len(out) <= max_out, lib.replace(' @', ''))


def abrev(lib, maxi=32):
    "abrège un libellé avec une longueur maximale (32 par défaut)"
    # suppression des accent et traits d'union
    lib = unidecode(lib).upper()
    # on ne garde que lettres et chiffres
    lib = re.sub(r'[^A-Z0-9]', ' ', lib).replace('  ', ' ')
    orig = lib  # on conserve le libellé original avant abréviation

    # soyons optimistes !
    if len(lib) <= maxi:
        return lib

    # 1 - abréviation du type de voie
    for r in regles:
        if r['etape'] == 1:
            lib = re.sub('^'+r['long'], r['court'], lib, count=1)

    out, ok, prev = abrev_out(orig, lib, maxi)
    if ok:
        return(out)
    else:
        lib = prev

    # 2 - abréviation des titres militaires, religieux et civils
    for n in range(0, 2):
        for r in regles:
            if r['etape'] == 2:
                lib = re.sub(" "+r['long']+" ", " "+r['court']+" ",
                             lib, count=1)
    if debug:
        print('2:', lib)

    out, ok, prev = abrev_out(prev, lib, maxi)
    if ok:
        return(out)
    else:
        lib = prev

    # 4 - abréviations générales
    for n in range(0, 3):
        for r in regles:
            if r['etape'] == 4:
                lib = re.sub("(^| )"+r['long']+" ", " "+r['court'].lower()+" ",
                             lib, count=1).strip()
    if debug:
        print('4:', lib)

    out, ok, prev = abrev_out(prev, lib, maxi)
    if ok:
        return(out)
    else:
        lib = prev

    # 5 - abréviation type de voies
    for n in range(0, 2):
        for r in regles:
            if r['etape'] == 5:
                lib = re.sub(" "+r['long'].strip()+" ",
                             " "+r['court'].strip().lower()+" ", lib, count=1)
        for r in regles:
            if r['etape'] == 1:
                lib = re.sub(" "+r['long'].strip()+" ",
                             " "+r['court'].strip().lower()+" ", lib, count=1)
    if debug:
        print('5:', lib)

    out, ok, prev = abrev_out(prev, lib, maxi)
    if ok:
        return(out)
    else:
        lib = prev

    # 3 - abréviations prénoms sauf pour ST prénoms
    mots = lib.split()
    for n in range(1, len(mots)-1):
        m = mots[n]
        if mots[n-1][0:5] != 'SAINT':
            for r in regles:
                if r['etape'] == 3:
                    m2 = re.sub('^'+r['long']+'$',
                                r['court'].lower(), m, count=1)
                    if m != m2:
                        lib = re.sub(" "+m+" ", " "+m2+" ", lib, count=1)
                        if debug:
                            print('3:', lib)

    out, ok, prev = abrev_out(prev, lib, maxi)
    if ok:
        return(out)
    else:
        lib = prev

    # 5bis - abréviation type de voies
    # for r in regles:
    #     if r['etape']==5:
    #         lib = re.sub("^"+r['long']+" ",r['court'].lower()+" ",lib, count=1)
    #         if len(lib)<=maxi:
    #             return lib
    # for r in regles:
    #     if r['etape']==5:
    #         lib = re.sub("^"+r['long']+" ",r['court'].lower()+" ",lib, count=1)
    #         if len(lib)<=maxi:
    #             return lib

    # 6 - abréviation saint/sainte et prolonge(e)/inférieur(e)
    for n in range(0, 2):
        for r in regles:
            if r['etape'] == 6:
                lib = re.sub(r['long'], r['court'].lower(), lib, count=1)
    if debug:
        print('6:', lib)

    out, ok, prev = abrev_out(prev, lib, maxi)
    if ok:
        return(out)
    else:
        lib = prev

    # 5bis - type de voie en début...
    for n in range(0, 1):
        for r in regles:
            if r['etape'] == 5:
                lib = re.sub("^"+r['long'].strip()+" ",
                             r['court'].strip().lower()+" ", lib, count=1)
    out, ok, prev = abrev_out(prev, lib, maxi)
    if ok:
        return(out)
    else:
        lib = prev

    lib = lib.replace(' @', '')

    # 9 - remplacement des particules des noms propres
    #     pour ne pas les supprimer
    for n in range(0, 1):
        for r in regles:
            if r['etape'] == 9:
                lib = re.sub(r['long'], r['court'], lib, count=1)
    if debug:
        print('9:', lib)

    # 10 - élimination des articles
    for r in range(0, 6):
        lib = re.sub(r" (LE|LA|LES|AU|AUX|DE|DU|DES|D|ET|A|L|SUR|EN) ",
                     " ", lib, count=1)
        if len(lib) <= maxi:
            return lib
    if debug:
        print('10:', lib)

    # 11 - abréviations résiduelle
    for m in lib.split():
        if m == m.upper() and len(m) > 1 and m[0] >= 'A':
            lib = re.sub(" "+m+" ", " "+m[0]+" ", lib, count=1)
            if len(lib) <= maxi:
                return lib
    if debug:
        print('11:', lib)

    # 12 - élimination des articles
    for r in range(0, 4):
        lib = re.sub(r" (le|la|les|au|aux|de|du|des|d|et|a|l|sur) ",
                     " ", lib, count=1)
        if len(lib) <= maxi:
            return lib
    if debug:
        print('12:', lib)

    return lib


if __name__ == "__main__":
    if len(sys.argv) == 1:
        if select.select([sys.stdin, ], [], [], 0.0)[0]:
            lines = sys.stdin.readlines()
            for l in lines:
                l = l.replace('\n', '')
                print(abrev(l).upper())
        else:
            print("""Usage:  normadresse.py adresse ou fichier.csv
        normadresse.py 'BOULEVARD DU MARECHAL JEAN MARIE DE LATTRE DE TASSIGNY'
        normadresse.py test.csv""")
    else:
        if bool(re.search('.csv$', sys.argv[1])):
            n = 0
            with open(sys.argv[1]) as test_csv:
                for t in csv.DictReader(test_csv):
                    a = abrev(t['nom'])
                    if len(t) == 1:
                        print(a.upper())
                    elif a.upper() != t['lib_voie']:
                        print(t['nom'],)
                        print(t['lib_voie'])
                        print('%s  (%s)' % (a, len(a)))
                        print()
                        n = n+1
            if n > 0:
                print(n)
        else:
            debug = (sys.argv[1] != sys.argv[1].upper())
            print(abrev(sys.argv[1].upper()).upper())
