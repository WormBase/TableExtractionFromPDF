# TableExtractionFromPDF
Extacting tables from PDFs using AWS textract


Many scientific papers contains tables, and it is a challenging task to identify tables in PDF files, and then extract them. After benchmarking several different approaches, we implemented this solution, which uses the AWS Textract service.

Function:
- Input a PDF of a scientific paper
- The script uses pdf2image to split the PDF, and convert each page of the PDF to a .png file (which is saved)
- Each .png file is then uploaded to s3, and any table in there is extracted using AWS Textract
- The textract output is sprawling, so it is munged, and only table information is extracted and saved in a pandas dataframe
- All pandas dataframes are saved as tsv files locally

Run it like this:
python pdf_table_extraction.py -i Example/WBPaper00046820_Thompson15.pdf -p WBPaper00046820 -o Example -b textract-console-eu-west-2-21d8e897-7155-4abd-bd05-54d63c21695b 

running with -h will show options and definitions



Configuration

To run this script, you have to have an AWS IAM user, who has been authorised to use the AWS textract service. You also need access to an S3 bucket to store files in temporarily. Here are some specific instructions if you are new to AWS Services: https://docs.aws.amazon.com/textract/latest/dg/getting-started.html. You can process 100 pages free of charge/month, but the script will cost a few pennies to run for the s3 usage.

The script uses a range of Python modules. The file Requirements.txt lists the external Python packages required to run it.




Scripts: 

pdf_table_extraction.py  -  runnable which wraps the other commands

tablex/pdf2image_runner.py - PDF to image conversion

tablex/text_extraction_AWS.py - subroutines for uploading to s3, text extraction and output parsing

tablex/text_extraction_AWS_multip.py - non-functional script, which could be developed if parallell processing was needed. Typically, processing a single paper only takes a few seconds, but the option is there to cut processing times if needed.


