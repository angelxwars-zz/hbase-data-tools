import logging
import sys

from tools.hbase.clean_db import CleanDB

from tools.extract.extract import Extract
from tools.hbase.create_table import CreateTable
from tools.upload.upload import Upload


class HAcid:

    TOOLS = {
       "upload": Upload,
        "extract": Extract,
        "delete_db": CleanDB,
        "create_table": CreateTable
    }

    def __init__(self):

        # Comprobamos el numero de argumentos TODO
        # TODO ver que no peta con solo el nombre del fichero
        # poner bonito: tool F C file(optional)
        if len(sys.argv) >= 1:
            self.module = sys.argv[1]
            self.arguments = sys.argv[2:]


    def run_job(self):
        if self.module in self.TOOLS:
            self.tool_instance = self.TOOLS[self.module](params=self.arguments)
            self.tool_instance.run_job()


logging.basicConfig(level=logging.DEBUG)
ht = HAcid()
ht.run_job()

