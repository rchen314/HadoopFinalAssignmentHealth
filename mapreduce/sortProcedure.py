#!/usr/bin/python

import sys
import re

"""SortProcedure

Takes a text file of the form

   Procedure   Cost
   
and regenerates it so it is sorted by cost.
   
"""

def SecondElement(tuple):

  return tuple[1]
  

def sort_by_price(filename):
  """
  Open a file and create a dictionary based on the procedure and cost pairs.  Then sort
  that dictionary and write the sorted output to the filename with "sorted" appended to it.
  """
  
  # Initialize variables
  costTable = {}
  sortedTable = {}
  numEntries = 0
  
  # Open file for reading and populate dictionary
  f = open(filename, "rU")
  text = f.read()
  entries1 = re.findall(r'(\d\d\d - \D+)(\d+.\d+)', text)
  entries2 = re.findall(r'(\d\d\d - \D+96\+ HOURS\D+)(\d+.\d+)', text)
  entries3 = re.findall(r'(\d\d\d - \D+<96 HOURS\D+)(\d+.\d+)', text)
  entries4 = re.findall(r'(\d\d\d - \D+4\+ VESSELS\D+)(\d+.\d+)', text)
  entries = entries1 + entries2 + entries3 + entries4
  for entry in entries:
    procedure = entry[0]
    cost = entry[1]
    costTable[procedure] = float(cost)
    numEntries += 1
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
  print "\n\nWrote " + str(numEntries) + " entries to " + outputFileName
  
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
