import sys
import logging
import subprocess
import csv, zipfile, sqlite3
from argparse import ArgumentParser
import pandas as pd

logging.basicConfig(filename="log/process_file.log", format = u'LINE:%(lineno)d# %(levelname)s %(asctime)s %(message)s', level = logging.INFO)


def main():
    parser = ArgumentParser()
    parser.add_argument("-f", "--rawfile", dest="rawfilename",
        help="CSV or TXT data filename", metavar="FILE")
    parser.add_argument("-u", "--user", dest="username",
        help="Username", metavar="USERNAME")
    parser.add_argument("-s", "--dbfile", dest="dbfilename",
        help="DB filename", metavar="DBFILENAME")

    args = parser.parse_args()
    if not args.rawfilename or not args.username or not args.dbfilename:
        logging.error('filename, username or dbfilename arguments are missing')
        parser.print_help()
        sys.exit(1)

    rawfilename = args.rawfilename
    username    = "userdata"
    dbfilename  = args.dbfilename

    logging.info("processing file %s" % rawfilename)

    subprocess.call(['sed', '-i', '/^#/ d', rawfilename]) # remove lines starting with special character
    if rawfilename.endswith('.txt'):
    	logging.info("Converting txt file %s to CSV" % rawfilename)
    	subprocess.call(['sed', '-i', 's/[ \t]/,/g', rawfilename]) # replacing tabs with comma
	with file(rawfilename, 'r') as original: data = original.read()
	with file(rawfilename, 'w') as modified: modified.write("RSID,CHROMOSOME,POSITION,RESULT\n" + data)

    insert_csv_to_db(dbfilename, rawfilename, username) # instert csv data to database
    logging.info("processing complete")


def insert_csv_to_db(dbfilename, rawfilename, username):
    logging.info("Loading CSV file %s" % rawfilename)
    try:
        with open(rawfilename,'rb') as fin:
            dr = csv.DictReader(fin)
            csv_db = [(i['RSID'], i['CHROMOSOME'],i['POSITION'],i['RESULT']) for i in dr]
    except Exception as e:
        logging.error("Opening CSV failed. Original error was: " + str(e))
        sys.exit(1)

    try:
        logging.info("Opening database file %s" % dbfilename)
        con = sqlite3.connect(dbfilename)
        cur = con.cursor()
    except Exception as e:
        logging.error("Opening DATABASE failed. Original error was: " + str(e))
        sys.exit(1)

    #import pdb
    #pdb.set_trace()

    query = """INSERT INTO {table} VALUES (?, ?, ?, ?)""".format(table=username)
    try:
        logging.debug("Creating table and inserting data to database")
        cur.execute("DROP TABLE IF EXISTS %s" % username)
        cur.execute("CREATE TABLE %s(ID CHARACTER(15) PRIMARY KEY, CHROM CHARACTER(1), POS INTEGER, RESULT CHAR(2))" % username)
        #cur.executemany("INSERT INTO %s VALUES (?, ?, ?, ?);" % username, csv_db)
        cur.executemany(query, (csv_db))
        con.commit()
        con.close()
    except Exception as e:
        logging.error("Writing DATABASE failed. Original error was: " + str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
