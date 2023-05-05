from pyspark import SparkContext
import sys
import random
from functools import partial

random.seed(123) #Fijamos una semilla

'''
Programa que toma como entrada el nombre de fichero de entrada, el nombre de fichero de salida y un porcentaje (entre 0 y 1).
El archivo de texto de salida consiste en la seleccion de un porcentaje de las lineas del archivo de entrada
de forma aleatoria
'''


def escoger(line, p):
    r = random.random()
    if r < p:
        return True
    else:
        return False

def main(sc, filename, out_filename, porcentaje):
    pp = float(porcentaje)
    data = sc.textFile(filename)
    escoger_p = partial(escoger, p = pp)
    lines_rdd = data.filter(escoger_p)

    with open(out_filename, 'w') as salida:
        for line in lines_rdd.collect():
            salida.write(line)
            salida.write('\n')

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        with SparkContext() as sc:
            sc.setLogLevel("ERROR")
            main(sc, sys.argv[1], sys.argv[2], sys.argv[3])