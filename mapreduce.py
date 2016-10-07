# -*-coding:Latin-1 -*

import os
import re

def map():
    tabmap = []
    fichier = input("Veuillez renseigner le fichier à analyser:\n")
    """Ouverture du fichier texte à analyser"""
    with open(fichier, 'r') as f:
        """ Recherche de tous les mots présent dans le texte grâce aux expressions régulière"""
        m = re.findall('[A-Za-z]{0,}\w+', f.read())
        print('Votre fichier contient:', len(m), "mots.")
        os.system("pause")
        for i in range (0, len(m)):
            tabmap.append([0] * 2)
            """"On remplit un tableau avec chaque mot (comme clé) et la valeur 1 associée"""
            tabmap[i][0] = m[i]
            tabmap[i][1] = 1
            print('{0} \t\t {1}'.format(tabmap[i][0], tabmap[i][1]))
    f.close()
    return tabmap

def reduce(word, tabword):
    tabreduce = []
    """ Création d'un nouveau tableau associant la clé (le mot) avec son nombre d'occurence dans le texte"""
    tabreduce.append(word)
    tabreduce.append(tabword.count(1))
    return tabreduce

def main():
    print('***** MAP *****\n')
    tabmap = map()
    print( '\n \n' )
    print('***** REDUCE *****\n')
    """Création d'une clé et du nombre d'occurance pour tester la fonction reduce"""
    word = "Peter"
    tabword = []
    for i in range (0, 6):
        tabword.append(1)
    tabreduce = reduce(word, tabword)
    print('{0} \t\t {1}'.format(tabreduce[0], tabreduce[1]))

main()
