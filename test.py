import subprocess as sp
from os import system, chdir, getcwd

if getcwd() != 'C:\\Users\\aafra\\Desktop\\dev_env\\database':
	chdir('database')

inputfile = open('input.txt', 'r')

system("gcc -o db_dev db_dev.c")
output = sp.check_output(['db_dev.exe', 'emptydb.db'], stdin=inputfile, timeout=0.01)

outputfile = open('output.txt', 'wb')
outputfile.write(output)
outputfile.close()

cleardb = open('emptydb.db', 'w')
cleardb.close()
inputfile.close()