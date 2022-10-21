#!/usr/bin/env python
from __future__ import print_function
import sys
import os.path
import argparse
from tablex.pdf2image_runner import pdf2pngs
import tablex.text_extraction_AWS
import boto3
from botocore.exceptions import ClientError
import os
from pathlib import Path

'''

Script to identify and extract all tables from a PDF using AWS textract service


'''

epi = ('\
    \n\
        Give an input PDF file, a prefix, and a base path\n\
        Output will be a set of numbered tables in tsv format\n\
     \n\
    \n\
')


# Describe what the script does
parser = argparse.ArgumentParser(description='This script writes tab-delimited files containing tables extracted from PDFs', epilog= epi, formatter_class=argparse.RawTextHelpFormatter)

# Get inputs
parser.add_argument('-i', '--input', default=None, dest='inp', action='store', required=True, help="PDF file path")
parser.add_argument('-p', '--prefix', default=None, dest='pfx', action='store', required=True, help="output prefix")
parser.add_argument('-o', '--output', default=None, dest='out', action='store', required=True, help="output folder")
parser.add_argument('-b', '--bucket', default='', dest='bu', action='store', required=True, help="s3 bucket name eg textract-console-eu-west-2-21d8e897-7155-4abd-bd05-54d63c21695b")
parser.add_argument('-r', '--region', default='eu-west-2', dest='reg', action='store', required=False, help="AWS region for bucket eg eu-west-2")



# Check for no input
if len(sys.argv)==1:
    parser.print_help()
    sys.exit(1)

args  = parser.parse_args()

BUCKET_NAME=args.bu
region=args.reg 

if not os.path.exists(args.out):
    os.makedirs(args.out)
    print ("Created folder", args.out)

# Check if output files exist
if not os.path.isfile(args.inp)==True:
    print("Cannot find input file ",args.inp)
    sys.exit(1)


# Take the PDF and convert each page to an image
files=pdf2pngs(args.inp,args.pfx,args.out)


# Upload the file to an s3 bucket

# Set AWS client
s3 = boto3.client('s3')
s3files=[]

for fil in files:
    filx=Path(fil).name
    s3 = boto3.client('s3')
    with open(fil, "rb") as f:
        #print (f, BUCKET_NAME,filx);
        s3.upload_fileobj(f, BUCKET_NAME, filx)
        s3files.append(filx)


# For each image, extract tables if there are any, and print the output in TSV file

for fil in s3files:
    #print ("Extracting ", fil)
    tablex.text_extraction_AWS.main(fil, BUCKET_NAME, region,args.out)
    print ("Done \n", fil)
    


quit()


