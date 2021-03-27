#!/usr/bin/env python

##	
#	Author: Mark Balantzyan
#	Last modified: Sept. 21, 2016
#	This file summarizes statistics for cells contained in a .csv file. 
#
#	Incorporates parsing of multiple .csv files. Summary is outputted into file "<file>_FRAME_< ALL | FRAMENUM >",
# 	where <file> is the name of the (respective) .csv file, this as the naming convention.
#	Output file(s) are created in SAME DIRECTORY AS .CSV FILES. Only one feature in the 
#	form of title (string) or column number (int) at a time can be fed into the script
#	on the command-line in this version.
#
#	Verification of mean-value calculation using the following Unix pipe, for, say, the feature in column 20:
#	
#		cat /path/to/<csvfile> | cut -d, -f 20 | awk '{ sum += $1 } END { print sum / NR }'
#
#	where <csvfile> is your .csv filename WITH filename extension.
##

import pandas as PD
from optparse import OptionParser
import csv

featarray = PD.DataFrame([])
i = 0
featureorcol_ = None
count = 0
patharr = []

#options parsing
parser = OptionParser()
parser.add_option("-c", "--csv", action="store", type="string", dest="csvfiles", help="Single or double-quotes enclosing one or more paths to respective csv files containing data", metavar="INPUT")
parser.add_option("-f", "--frame", action="store", type="int", dest="frame", help="(optional) frame for script to analyze. By default, all frames are summarized.", metavar="FRAME")
parser.add_option("-F", "--feature", action="store", type="string", dest="featureorcol", help="(optional) name of one feature to average (over frame, if -f flag given), or one .csv column number", metavar="FEATURE | COL")

#options parsing
(options, args) = parser.parse_args()
if options.csvfiles:
	csvfiles = options.csvfiles
elif not options.csvfiles:
	parser.error('We need at least one .csv file to parse after the -c or --csv parameter.')
	exit()
if options.frame:
	frame_ = options.frame
elif not options.frame:
	frame_ = None
if options.featureorcol:
	featureorcol_ = options.featureorcol
	featarray = featureorcol_.split()

# Stores features passed in, while determining whether they are column numbers or feature-names.
for item in featarray:
	try:
		if type(int(item)) is int:
			featarray[i] = int(item)
			++i
	except ValueError:
		featarray[i] = item
		++i			
		
def feat_compute(filepath, outfile):

		# initial Dataframe
	try:
		df = PD.read_csv(filepath)
		# j is counter used in evaluating the program for each feature given on the command line.
		j = 0
		# Dataframe after filtering by frame
		df_fullslice = PD.DataFrame([])
		df_wanted = PD.DataFrame([])
		
		if frame_ > max(df.Metadata_FrameNumber) or frame_ < 0:
			print('Frame inputted does not exist in inputted .csv file(s).')
			exit()

		if frame_ and featureorcol_:
			try:
				for item in featarray:
					if type(item) is int:
						for j in range(len(featarray)):
							df_fullslice = df.loc[df.Metadata_FrameNumber == frame_]
							df_wanted = df_fullslice.iloc[:,featarray[j]]
							outfile.write(str(df_wanted.describe()))
					if type(item) is str:
						for j in range(len(featarray)):
							df_fullslice = df.loc[df.Metadata_FrameNumber == frame_]
							df_wanted = df_fullslice.loc[:,featarray[j]]
							outfile.write(str(df_wanted.describe()))
			except IndexError:
				print('The column number you gave is not in the file.')
				exit()
			except KeyError:
				print('The feature-title you gave is not in the file.')
				exit()
			# exceptions given incorrect argument for -F flag
	
		# if no frame option and no feature or no column number given
		elif not frame_ and not featureorcol_:
			summarydf_all_noframenofeature = df.describe()
			outfile.write(str(summarydf_all_noframenofeature))

		# if feature or column of feature given, but no frame option			
		elif not frame_ and featureorcol_:
			try:
				for item in featarray:
					if type(item) is int:
						for j in range(len(featarray)):
							df_fullslice = df.iloc[:,featarray[j]]
							outfile.write(str(df_fullslice.describe()))
					if type(item) is str:
						for j in range(len(featarray)):
							df_fullslice = df.loc[:,featarray[j]]
							outfile.write(str(df_fullslice.describe()))
			except IndexError:
				print('The column number you gave is not in the file.')
				exit()
			except KeyError:
				print('The feature-title you gave is not in the file.')
				exit()
	
						
		# if frame given but not feature or column number			
		elif frame_ and not featureorcol_:
			df_all_withframe_wanted = df[df.Metadata_FrameNumber == frame_]
			summarydf_all_withframe = df_all_withframe_wanted.describe()
			outfile.write(str(summarydf_all_withframe))
	except OSError:
		print('The .csv file you gave is either mistyped or not present at that location. Please correctly re-specify.')
		exit()
	
# PROGRAM BODY
def main(featarray,csvfiles,frame_,featureorcol_,patharr):

	# path array made to contain paths given in quotes on command line	
	if not csvfiles:
		parser.error('We need at least one .csv file to parse after the -c or --csv parameter.')
		exit()
	else:
		patharr = csvfiles.split()

	for element in patharr:
		if frame_:

			with open(element+'_FRAME_'+(str(frame_)),'a') as outfile:
	
				feat_compute(str(element), outfile)

		elif not frame_:

			with open(element+'_FRAME_'+'ALL','a') as outfile:

				feat_compute(str(element), outfile)
# END OF PROGRAM BODY

# executes script for each filepath given on command line.
if not csvfiles:
	parser.error('We need at least one .csv file to parse after the -c or --csv parameter.')
else:
	main(featarray,csvfiles,frame_,featureorcol_,patharr)
