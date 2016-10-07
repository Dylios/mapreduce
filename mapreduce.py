# -*-coding:Latin-1 -*

import os
import re

def map():
    tabmap = []
    fichier = input("Veuillez renseigner le fichier à analyser:\n")
    with open(fichier, 'r') as f:
        m = re.findall('[A-Za-z]{0,}\w+', f.read())
        print('Votre fichier contient:', len(m))
        os.system("pause")
        for i in range (0, len(m)):
            tabmap.append([0] * 2)
            #print(tabmap)
            #os.system("pause")
            tabmap[i][0] = m[i]
            tabmap[i][1] = 1
            print('{0} \t\t {1}'.format(tabmap[i][0], tabmap[i][1]))
            # tabmap[i][0] = m[i]
            # tabmap[i][1] = '1'
            # print(tabmap[i][0]+"=======>"+tabmap[i][1]+"\n")
    f.close()
    return tabmap

def reduce(word, tabword):
    tabreduce = []
    tabreduce.append(word)
    tabreduce.append(tabword.count(1))
    return tabreduce

def main():
    print('***** MAP *****\n')
    tabmap = map()
    print( '\n \n' )
    print('***** REDUCE *****\n')
    word = "Peter"
    tabword = []
    for i in range (0, 6):
        tabword.append(1)
    tabreduce = reduce(word, tabword)
    print('{0} \t\t {1}'.format(tabreduce[0], tabreduce[1]))

main()
