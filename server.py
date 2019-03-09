import os
import subprocess
import sqlite3
import logging
import base64
from flask import jsonify, Flask
from flask import request
from flask import abort
from flask import Response
app = Flask(__name__)


logging.basicConfig(filename="log/server.log", format = u'LINE:%(lineno)d# %(levelname)s %(asctime)s %(message)s', level = logging.DEBUG)
logging.info("Starting Flask server...")

@app.route('/develfile',methods=['GET','POST'])
def develfile():
    if request.method == 'POST':
        username  = request.form['userName']
        password  = request.form['password']
        userfile  = username + ".csv"
        dbfile    = username + ".db"
        initialdb = 'initial.db'
        analyzedb = 'analyze.db'

        logging.debug("Request recived username %s" % username)
        logging.debug("Request recived password %s" % password)
        logging.debug("Size of data:" + str(len(request.form['rawdata'])))

        with open(userfile, 'w') as file:
                file.write(request.form['rawdata'].encode("UTF-8"))
        logging.info("Raw file saved")
        print "File saved."

        logging.info("Loading data to database from CSV")
        retcode =  subprocess.call(['python', 'process_file.py', '-f%s' % userfile, '-u%s' % username, '-s%s' % dbfile])
        if (retcode <> 0):
            logging.error("Error processing RAW data file")
            #abort('Error processing file')
            return "Something is wrong...", 500
        logging.info("Data loaded successfully")

        if os.path.exists(userfile):
            os.remove(userfile)
            logging.info("CSV file removed")
        else:
            logging.error("File does not exist")

        logging.info("Analizing database file")
        retcode =  subprocess.call(['python', 'analyze_file.py', '-f%s' % initialdb,'-u%s' % username, '-s%s' % dbfile, '-a%s' % analyzedb])
        if (retcode <> 0):
            logging.error("Error Analyzing RAW data file")
            #abort('Error processing file')
            return "Something is wrong...", 500
        logging.info("Analizing completed successfully")

        return "ok"
    else:
        return "Something is wrong..."

@app.route("/basiccounters", methods=['POST', 'GET'])
def basiccounters():
    if request.method == 'POST':
        username = request.form['userName']
        dbfile    = username + ".db"

        logging.info("Username %s" % username)
        logging.info("Check if DB file exists: %s" % dbfile)
	if not os.path.exists(dbfile):
            logging.error("DB file does not exists: %s" % dbfile)
            return "Something is wrong...", 500

        logging.info("Calling function to read data from DB")
        counters = get_basic_counters(dbfile)

        disease     = [i[0] for i in counters]
        porbability = [i[1] for i in counters]
        icons       = [base64.b64encode(i[2]) for i in counters]

        return jsonify({'Porbability' : porbability ,'Disease' : disease, 'Icons' : icons})


def get_basic_counters(dbfile):
    logging.info("Opening database file %s" % dbfile)
    con = sqlite3.connect(dbfile)
    cur = con.cursor()

    logging.debug("Reading data from database")
    cur.execute("SELECT * FROM disease")
    res = cur.fetchall()

    con.close()
    return (res)
