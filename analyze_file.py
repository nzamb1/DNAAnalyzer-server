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
    parser.add_argument("-a", "--analyzedb", dest="analyzedb",
        help="Analyzedb database", metavar="ANALYZEDB")

    args = parser.parse_args()
    if not args.initialdb or not args.username or not args.dbfilename or not args.analyzedb:
        logging.error('initialdb, username, dbfilename or analyzedb arguments are missing')
        parser.print_help()
        sys.exit(1)

    initialdb    = args.initialdb
    username     = args.username
    dbfilename   = args.dbfilename
    analyzedb    = args.analyzedb


    initializedb(initialdb, dbfilename, username)
    performanalyze(analyzedb, dbfilename, username)
    logging.info("processing complete")

def performanalyze(analyzedb, dbfilename, username):
    logging.info("Performing analyzis of user genome")
    try:
        logging.info("Opening database file for writing: %s" % dbfilename)
        con = sqlite3.connect(dbfilename)
        cur = con.cursor()
    except Exception as e:
        logging.error("Opening DATABASE failed. Original error was: " + str(e))
        sys.exit(1)
    try:
        logging.debug("Attaching analyze DB to user DB")
        cur.execute("ATTACH DATABASE '%s' AS analyze" % analyzedb)
        cur.execute("INSERT INTO disease_analyze SELECT t2.DISEASE_NAME, t1.ID, t1.RESULT, t2.MAGNITUDE FROM %s AS t1 INNER JOIN analyze.disease_list AS t2 ON (t1.id = t2.RSID) AND t1.RESULT = t2.RESULT;" % username)
        con.commit()
        con.close()
    except Exception as e:
        logging.error("Writing DATABASE failed. Original error was: " + str(e))
        sys.exit(1)

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
        cur.execute("DROP TABLE IF EXISTS disease_analyze")
        cur.execute("ATTACH DATABASE '%s' AS initdb" % initialdb)
        cur.execute("CREATE TABLE disease AS SELECT * FROM initdb.initial_disease WHERE 0" ) # copy TABLE schema of riscs and pic
        cur.execute("CREATE TABLE disease_analyze AS SELECT * FROM initdb.disease_analyze WHERE 0" ) # copy TABLE schema analyze result
        cur.execute("INSERT INTO main.disease SELECT * FROM initdb.initial_disease") # fill in table with pictures and initial values
        con.commit()
        con.close()
    except Exception as e:
        logging.error("Writing DATABASE failed. Original error was: " + str(e))
        sys.exit(1)

    logging.info("Writing data completed. DB close.")


if __name__ == "__main__":
    main()
