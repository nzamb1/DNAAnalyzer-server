import sys
import logging
import subprocess
import csv, sqlite3
from argparse import ArgumentParser

logging.basicConfig(filename="log/alalyze_file.log", format = u'LINE:%(lineno)d# %(levelname)s %(asctime)s %(message)s', level = logging.INFO)


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
    username     = "userdata"
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
        cur.execute("INSERT INTO disease_analyze SELECT t2.DISEASE_NAME, t2.DESCRIPTION, t1.ID, t1.RESULT, t2.MAGNITUDE FROM %s AS t1 INNER JOIN analyze.disease_list AS t2 ON (t1.id = t2.RSID) AND t1.RESULT = t2.RESULT;" % username)
        cur.execute("INSERT INTO traits_analyze SELECT t2.TRAIT_NAME, t2.DESCRIPTION, t1.ID, t1.RESULT, t2.MAGNITUDE FROM %s AS t1 INNER JOIN analyze.traits_list AS t2 ON (t1.id = t2.RSID) AND t1.RESULT = t2.RESULT;" % username)
        con.commit()
#        con.close()
    except Exception as e:
        logging.error("Performanalyze. Writing DATABASE failed. Original error was: " + str(e))
        sys.exit(1)

    try:
        logging.info("Updating disease probability")
        con.row_factory = lambda cursor, row: row[0]
        cur = con.cursor()
        cur.execute("SELECT DISTINCT DISEASE_NAME FROM disease_analyze ORDER BY 1")
        diseases = cur.fetchall()
        for disease in diseases:
            cur.execute('SELECT MAGNITUDE FROM disease_analyze WHERE DISEASE_NAME = "%s"' % disease)
            magnitude = cur.fetchall()
            setmagnitude = max(magnitude) * (len(magnitude)*0.1+1)
            if setmagnitude >= 3:
                cur.execute('UPDATE disease SET PROBABILITY = 3 WHERE DISEASE_NAME = "%s"' % disease)
            elif setmagnitude >= 2:
                cur.execute('UPDATE disease SET PROBABILITY = 2 WHERE DISEASE_NAME = "%s"' % disease)
            elif setmagnitude > 1:
                cur.execute('UPDATE disease SET PROBABILITY = 1 WHERE DISEASE_NAME = "%s"' % disease)
        con.commit()
    except Exception as e:
        logging.error("Updating Disease probability failed. Original error was: " + str(e))
        sys.exit(1)

    try:
        logging.info("Updating traits probability")
        con.row_factory = lambda cursor, row: row[0]
        cur = con.cursor()
        cur.execute("SELECT DISTINCT TRAIT_NAME FROM traits_analyze ORDER BY 1")
        traits = cur.fetchall()
        for trait in traits:
            cur.execute('SELECT MAGNITUDE FROM traits_analyze WHERE TRAIT_NAME = "%s"' % trait)
            magnitude = cur.fetchall()
            setmagnitude = max(magnitude) * (len(magnitude)*0.1+1)
            if setmagnitude >= 3:
                cur.execute('UPDATE traits SET PROBABILITY = 3 WHERE TRAIT_NAME = "%s"' % trait)
            elif setmagnitude >= 2:
                cur.execute('UPDATE traits SET PROBABILITY = 2 WHERE TRAIT_NAME = "%s"' % trait)
            elif setmagnitude > 1:
                cur.execute('UPDATE traits SET PROBABILITY = 1 WHERE TRAIT_NAME = "%s"' % trait)
        con.commit()
    except Exception as e:
        logging.error("Updating traits probability failed. Original error was: " + str(e))
        sys.exit(1)

    con.close()

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
        cur.execute("DROP TABLE IF EXISTS traits")
        cur.execute("DROP TABLE IF EXISTS traits_analyze")
        cur.execute("ATTACH DATABASE '%s' AS initdb" % initialdb)
        cur.execute("CREATE TABLE disease AS SELECT * FROM initdb.initial_disease WHERE 0" ) # copy TABLE schema of riscs and pic
        cur.execute("CREATE TABLE disease_analyze AS SELECT * FROM initdb.disease_analyze WHERE 0" ) # copy TABLE schema analyze result
        cur.execute("CREATE TABLE traits AS SELECT * FROM initdb.initial_traits WHERE 0" ) # copy TABLE schema of riscs and pic
        cur.execute("CREATE TABLE traits_analyze AS SELECT * FROM initdb.traits_analyze WHERE 0" ) # copy TABLE schema analyze result
        cur.execute("INSERT INTO main.disease SELECT * FROM initdb.initial_disease") # fill in table with pictures and initial values
        cur.execute("INSERT INTO main.traits SELECT * FROM initdb.initial_traits") # fill in table with pictures and initial values
        con.commit()
        con.close()
    except Exception as e:
        logging.error("Initializedb. Writing DATABASE failed. Original error was: " + str(e))
        sys.exit(1)

    logging.info("Writing data completed. DB close.")


if __name__ == "__main__":
    main()
