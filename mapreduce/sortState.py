#!/usr/bin/python

import sys
import re

"""SortState

Takes a text file of the form

   State   Cost
   
and regenerates it so it is sorted by cost.
   
"""

def SecondElement(tuple):

  return tuple[1]
  

def sort_by_price(filename):
  """
  Open a file and create a dictionary based on the state and cost pairs
  """
  
  # Initialize variables
  costTable = {}
  sortedTable = {}
  
  # Open file for reading and populate dictionary
  f = open(filename, "rU")
  for line in f:
    entries = line.split()
    state = entries[0]
    cost = entries[1]
    costTable[state] = float(cost)
    #print "costTable[" + state + "] = " + cost
  f.close 

  # Sort values
  sortedTable = sorted(costTable.items(), reverse=True, key=SecondElement)

  # Write to file
  outputFileName = filename + ".sorted"   
  f = open(outputFileName, "w")
  for tuple in sortedTable:
     f.write(tuple[0])
     f.write(" ")
     f.write(str(tuple[1]))
     f.write("\n")
     print tuple[0], ":", tuple[1]  
  f.close  
  print "\nGenerated " + outputFileName
  
def main():
  # This command-line parsing code is provided.
  # Make a list of command line arguments, omitting the [0] element
  # which is the script itself.
  args = sys.argv[1:]
      
  if not args:
    print 'usage: file'
    sys.exit(1)

  sort_by_price(sys.argv[1])
  
if __name__ == '__main__':
  main()
