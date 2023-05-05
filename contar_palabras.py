import string
from pyspark import SparkContext
import sys

'''
Programa que toma como entrada el nombre de fichero de entrada, el nombre de fichero de salida.
Imprime por pantalla el número de palabras que tiene el texto de entrada y en el de salida
escribe el resultado.
'''


def word_split(line):
    for c in string.punctuation:
        line = line.replace(c, ' ')
    line = line.split()
    return line

def main(sc, filename, out_filename):
    data = sc.textFile(filename)
    words_rdd = data.flatMap(word_split)
    result = words_rdd.count()
    print ('RESULTS------------------')
    print ('words_count', result)
    with open(out_filename, 'w') as salida:
        salida.write('RESULTS------------------\n')
        salida.write(f'words_count {result}')

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        with SparkContext() as sc:
            sc.setLogLevel("ERROR")
            main(sc, sys.argv[1], sys.argv[2])
