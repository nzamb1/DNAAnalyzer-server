import sys
import logging
import subprocess
import csv, sqlite3
from argparse import ArgumentParser

logging.basicConfig(filename="alalyze_file.log", format = u'LINE:%(lineno)d# %(levelname)s %(asctime)s %(message)s', level = logging.INFO)


def main():
    parser = ArgumentParser()
    parser.add_argument("-f", "--initialdb", dest="initialdb",
        help="Initial database", metavar="INITIALDATABASE")
    parser.add_argument("-u", "--user", dest="username",
        help="Username", metavar="USERNAME")
    parser.add_argument("-s", "--dbfile", dest="dbfilename",
        help="DB filename", metavar="DBFILENAME")

    args = parser.parse_args()
    if not args.initialdb or not args.username or not args.dbfilename:
        logging.error('initialdb, username or dbfilename arguments are missing')
        parser.print_help()
        sys.exit(1)

    initialdb    = args.initialdb
    username     = args.username
    dbfilename   = args.dbfilename


    initializedb(initialdb, dbfilename, username)
    logging.info("processing complete")


def initializedb(initialdb, dbfilename, username):
    logging.info("Coping data from initial DB to user DB")

    try:
        logging.info("Opening database file for writing: %s" % dbfilename)
        con = sqlite3.connect(dbfilename)
        cur = con.cursor()
    except Exception as e:
        logging.error("Opening DATABASE failed. Original error was: " + str(e))
        sys.exit(1)

    try:
        logging.debug("Attaching initial DB to user DB")
        cur.execute("DROP TABLE IF EXISTS disease")
        cur.execute("ATTACH DATABASE '%s' AS initdb" % initialdb)
        cur.execute("CREATE TABLE disease AS SELECT * FROM initdb.initial_disease WHERE 0" )
        cur.execute("INSERT INTO main.disease SELECT * FROM initdb.initial_disease")
        con.commit()
        con.close()
    except Exception as e:
        logging.error("Writing DATABASE failed. Original error was: " + str(e))
        sys.exit(1)

    logging.info("Writing data completed. DB close.")


if __name__ == "__main__":
    main()
