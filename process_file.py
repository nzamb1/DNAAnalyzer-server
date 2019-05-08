import sys
import logging
import subprocess
import csv, zipfile, sqlite3
from zipfile import ZipFile
from argparse import ArgumentParser

logging.basicConfig(filename="log/process_file.log", format = u'LINE:%(lineno)d# %(levelname)s %(asctime)s %(message)s', level = logging.INFO)


def main():
    parser = ArgumentParser()
    parser.add_argument("-f", "--rawfile", dest="rawfilename",
        help="CSV or ZIP RAW data filename", metavar="FILE")
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
    username    = args.username
    dbfilename  = args.dbfilename

    logging.info("processing file %s" % rawfilename)
    if zipfile.is_zipfile(rawfilename):
        logging.info("Provided file is ZIP archive. Extracting...")

	with ZipFile(rawfilename, 'r') as zipObj:
	   # Get a list of all archived file names from the zip
	   listOfFileNames = zipObj.namelist()
	   # Iterate over the file names
	   for fileName in listOfFileNames:
	       # Check filename endswith csv
	       if fileName.endswith('.csv'):
		   # Extract a single file from zip
		   rawfilename = zipObj.extract(fileName)
    else:
        logging.info("Provided file is NOT archive. Continuing...")



    subprocess.call(['sed', '-i', '/^#/ d', rawfilename]) # remove lines starting with special character
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

    try:
        logging.debug("Creating table and inserting data to database")
        cur.execute("DROP TABLE IF EXISTS %s" % username)
        cur.execute("CREATE TABLE %s(ID CHARACTER(15) PRIMARY KEY, CHROM CHARACTER(1), POS INTEGER, RESULT CHAR(2))" % username)
        cur.executemany("INSERT INTO %s VALUES (?, ?, ?, ?);" % username, csv_db)
        con.commit()
        con.close()
    except Exception as e:
        logging.error("Writing DATABASE failed. Original error was: " + str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
