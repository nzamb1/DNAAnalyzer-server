import sys
import logging
import subprocess
import csv, sqlite3
from argparse import ArgumentParser

logging.basicConfig(filename="process_file.log", format = u'LINE:%(lineno)d# %(levelname)s %(asctime)s %(message)s', level = logging.INFO)


def main():
    parser = ArgumentParser()
    parser.add_argument("-f", "--file", dest="filename",
        help="CSV RAW data filename", metavar="FILE")
    parser.add_argument("-u", "--user", dest="username",
        help="Username", metavar="USERNAME")

    args = parser.parse_args()
    if not args.filename or not args.username:
        logging.error('filename or username argument is missing')
        parser.print_help()
        sys.exit(1)

    filename = args.filename
    username = args.username
    logging.info("processing file %s" % filename)

    subprocess.call(['sed', '-i', '/^#/ d', filename]) # remove lines starting with special character
    insert_csv_to_db(filename, username) # instert csv data to database
    logging.info("processing complete")


def insert_csv_to_db(filename, username):
    logging.info("Loading CSV file %s" % filename)
    with open(filename,'rb') as fin:
        dr = csv.DictReader(fin)
        to_db = [(i['RSID'], i['CHROMOSOME'],i['POSITION'],i['RESULT']) for i in dr]

    logging.info("Opening database file %s" % filename)
    con = sqlite3.connect('nzamb.lite')
    cur = con.cursor()

    logging.debug("Creating table and inserting data to database")
    cur.execute("DROP TABLE IF EXISTS %s" % username)
    cur.execute("CREATE TABLE %s(ID CHARACTER(15) PRIMARY KEY, CHROM CHARACTER(1), POS INTEGER, RESULT CHAR(2))" % username)
    cur.executemany("INSERT INTO %s VALUES (?, ?, ?, ?);" % username, to_db)
    con.commit()
    con.close()


if __name__ == "__main__":
    main()
