
from pdf2image import convert_from_path, convert_from_bytes

from pdf2image.exceptions import (
    PDFInfoNotInstalledError,
    PDFPageCountError,
    PDFSyntaxError
)

import tempfile




def pdf2pngs(pdf,pfx,out):

    files=[]

    with tempfile.TemporaryDirectory() as path:
        images_from_path = convert_from_path(pdf, output_folder=path)

        # Save images in a folder
        i=1
        for im in images_from_path:
            fn = ''.join([out,'/', pfx , ".p",str(i),".png"])
            #print ("Saving file:", fn)
            im.save(fn)
            files.append(fn)
            i=i+1

    return files
        




