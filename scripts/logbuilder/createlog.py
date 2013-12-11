"""
Added script to create logs. Sintaxe is as following:
python -m py_compile createlog.py
python createlog.pyc [<number of cells>]

If there is no argument inserted, default number for cells is 100 (25MB file).
"""

import numpy as np
import string, sys, getopt, re
from itertools import cycle

src = []
opts, args = getopt.getopt(sys.argv,"")

with open("log_source.txt", "r") as infile:
	for line in infile:
		src.append(re.split('\t|\n| ',line))

it = cycle(src)

try:
	cell_limit = int(args[1])
except:
	cell_limit = 100

with open("log.txt", "w") as outfile:
	for c in range(1,cell_limit):
		cell = str(c)
		for y in range(1990,2013):
			year = str(y)
			for m in range(1,12):
				month = str(m)
				for d in range(1,30):
					for vals in src:
						day = str(d)
						#vals = it.next()
						outfile.write(\
						"C"+cell+","+day+"/"+month+"/"+year+","+vals[0]+","+vals[1]+","+"C"+cell+vals[2]+"\n"\
						)
