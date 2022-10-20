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
python ~/path/TableExtractionFromPDF/pdf_table_extraction.py -i WBPaper00030864_Andersen07.pdf -p WBPaper00030864 -o ~/Desktop/Table_OUT

running with -h will show options




Configuration

To run this script, you have to have an AWS user, who has been authorised to use the AWS textract service. You also need access to an S3 bucket to store files in temporarily.

The script uses a range of Python modules, and the software "poplar".


