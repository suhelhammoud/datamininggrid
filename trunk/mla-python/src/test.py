from util import *
from partition_data import addlines

print "d"

#spaced_to_arff("../data/chess.dat", "../data/chess.arff", False)

spaced_to_arff("../data/chess.dat", "../data/chess_text.arff", True)

addlines('../data/chess.dat', '../data/chess.lined')
