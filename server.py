import os
import subprocess
import sqlite3
import logging
import base64
from flask import jsonify, Flask
from flask import request
from flask import abort
from flask import Response
import zipfile
app = Flask(__name__)


logging.basicConfig(filename="log/server.log", format = u'LINE:%(lineno)d# %(levelname)s %(asctime)s %(message)s', level = logging.DEBUG)
logging.info("Starting Flask server...")

commaodbpath = "commondb/"
userdbpath   = "userdb/"

@app.route('/develfile',methods=['GET','POST'])
def develfile():
    if request.method == 'POST':
        username  = request.form['userName']
        password  = request.form['password']
        filetype  = request.form['filetype']
        userfile  = str(username + "." + filetype)
        dbfile    = userdbpath + username + ".db"
        initialdb = 'initial.db'
        analyzedb = 'analyze.db'

        logging.debug("Request recived username %s" % username)
        logging.debug("Request recived password %s" % password)
        logging.debug("Request recived filetype %s" % filetype)
        logging.debug("Size of data:" + str(len(request.form['rawdata'])))

        with open(userfile, 'wb') as file:
                #file.write(request.form['rawdata'].encode("UTF-8"))
                file.write(base64.decodestring(request.form['rawdata']))
        logging.info("Raw file saved")

	if filetype == 'zip':
        	logging.info("Zip file recived. Extracting...")
		zipobj = zipfile.ZipFile(userfile, 'r')
		userfile = zipobj.extract(zipobj.namelist()[0])
	
        #return "Something is wrong...", 500

        logging.info("Loading data to database from CSV")
        retcode =  subprocess.call(['python', 'process_file.py', '-f%s' % userfile, '-u%s' % username, '-s%s' % dbfile])
        if (retcode <> 0):
            logging.error("Error processing RAW data file")
            #abort('Error processing file')
            return "Something is wrong...", 500
        logging.info("Data loaded successfully")

        if os.path.exists(userfile):
            os.remove(userfile)
            logging.info("Uploaded file removed")
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
        dbfile    = userdbpath + request.form['userName'] + ".db"

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


@app.route("/trbasiccounters", methods=['POST', 'GET'])
def trbasiccounters():
    if request.method == 'POST':
        username = request.form['userName']
        dbfile    = userdbpath + request.form['userName'] + ".db"

        logging.info("Username %s" % username)
        logging.info("Check if DB file exists: %s" % dbfile)
	if not os.path.exists(dbfile):
            logging.error("DB file does not exists: %s" % dbfile)
            return "Something is wrong...", 500

        logging.info("Calling function to read data from DB")
        counters = traits_get_basic_counters(dbfile)

        traits     = [i[0] for i in counters]
        trporbability = [i[1] for i in counters]
        tricons       = [base64.b64encode(i[2]) for i in counters]

        return jsonify({'TrPorbability' : trporbability , 'Traits' : traits, 'TrIcons' : tricons})

@app.route("/corbasiccounters", methods=['POST', 'GET'])
def corbasiccounters():
    if request.method == 'POST':
        username = request.form['userName']
        dbfile    = userdbpath + request.form['userName'] + ".db"

        logging.info("Username %s" % username)
        logging.info("Check if DB file exists: %s" % dbfile)
	if not os.path.exists(dbfile):
            logging.error("DB file does not exists: %s" % dbfile)
            return "Something is wrong...", 500

        logging.info("Calling function to read data from DB")
        counters = coronavirus_get_basic_counters(dbfile)

        corvirus     = [i[0] for i in counters]
        corporbability = [i[1] for i in counters]
        coricons       = [base64.b64encode(i[2]) for i in counters]

        return jsonify({'CorPorbability' : corporbability , 'Corvirus' : corvirus, 'CorIcons' : coricons})
        #return jsonify({'TrPorbability' : trporbability , 'Traits' : traits, 'TrIcons' : tricons})

@app.route("/searchrsid", methods=['POST', 'GET'])
def searchrsid():
    usertablename= "userdata"
    rsid         = request.form['rsid']
    dbfile       = userdbpath + request.form['userName'] + ".db"

    logging.debug("Recived username: %s " % request.form['userName'])
    logging.debug("Recived rsid: %s " % request.form['rsid'])

    logging.info("Opening database file %s" % dbfile)
    try:
        con = sqlite3.connect(dbfile)
        cur = con.cursor()

        logging.debug("Reading data from database")
        query = "SELECT * FROM %s WHERE ID LIKE \"%s\" LIMIT 30" % (usertablename, "%" + rsid + "%")
        logging.debug(query)
        cur.execute(query)
        res = cur.fetchall()
        con.close()

    except Exception as e:
        logging.error("Error reading data from DB. Original error was: " + str(e))
        return "Error reading data from DB...", 500

    dbrsid      = [i[0] for i in res]
    chrom       = [i[1] for i in res]
    position    = [i[2] for i in res]
    result      = [i[3] for i in res]

    return jsonify({'Rsid' : dbrsid ,'Chromosome' : chrom, 'Position' : position, 'Result' : result})

@app.route("/getdiseasedetails", methods=['POST', 'GET'])
def getdiseasedetails():
    username     = request.form['userName']
    diseasename  = request.form['diseasename']

    dbfile       = userdbpath + username + ".db"

    logging.debug("Recived username: %s " % username)
    logging.debug("Recived diseasename: %s " % diseasename)
    try:
        logging.info("Opening database file %s" % dbfile)
        con = sqlite3.connect(dbfile)
        cur = con.cursor()

        logging.debug("Reading data from database")

        # DISEASE_NAME TEXT, DESCRIPTION TEXT, RSID TEXT, RESULT TEXT, MAGNITUDE REAL
        cur.execute('SELECT DESCRIPTION, RSID, RESULT FROM disease_analyze WHERE DISEASE_NAME = "%s"' % diseasename)
        res = cur.fetchall()
        con.close()
    except Exception as e:
        logging.error("Error reading data from DB. Original error was: " + str(e))
        return "Error reading data from DB...", 500

    description = [i[0] for i in res]
    rsid        = [i[1] for i in res]
    mutation    = [i[2] for i in res] 

    return jsonify({'Description' : description ,'Rsid' : rsid, 'Mutation' : mutation})

@app.route("/gettraitdetails", methods=['POST', 'GET'])
def gettraitdetails():
    username     = request.form['userName']
    traitname  = request.form['traitname']

    dbfile       = userdbpath + username + ".db"

    logging.debug("Recived username: %s " % username)
    logging.debug("Recived traitname: %s " % traitname)
    try:
        logging.info("Opening database file %s" % dbfile)
        con = sqlite3.connect(dbfile)
        cur = con.cursor()

        logging.debug("Reading data from database")

        # DISEASE_NAME TEXT, DESCRIPTION TEXT, RSID TEXT, RESULT TEXT, MAGNITUDE REAL
        cur.execute('SELECT DESCRIPTION, RSID, RESULT FROM traits_analyze WHERE TRAIT_NAME = "%s"' % traitname)
        res = cur.fetchall()
        con.close()
    except Exception as e:
        logging.error("Error reading data from DB. Original error was: " + str(e))
        return "Error reading data from DB...", 500

    description = [i[0] for i in res]
    rsid        = [i[1] for i in res]
    mutation    = [i[2] for i in res] 

    return jsonify({'Description' : description ,'Rsid' : rsid, 'Mutation' : mutation})

@app.route("/getcordetails", methods=['POST', 'GET'])
def getcordetails():
    username     = request.form['userName']
    corname  = request.form['corname']

    dbfile       = userdbpath + username + ".db"

    logging.debug("Recived username: %s " % username)
    logging.debug("Recived corname: %s " % corname)
    try:
        logging.info("Opening database file %s" % dbfile)
        con = sqlite3.connect(dbfile)
        cur = con.cursor()

        logging.debug("Reading data from database")

        # CORONAVIRUS_NAME TEXT, DESCRIPTION TEXT, RSID TEXT, RESULT TEXT, MAGNITUDE REAL
        cur.execute('SELECT DESCRIPTION, RSID, RESULT FROM coronavirus_analyze WHERE CORONAVIRUS_NAME = "%s"' % corname)
        res = cur.fetchall()
        con.close()
    except Exception as e:
        logging.error("Error reading data from DB. Original error was: " + str(e))
        return "Error reading data from DB...", 500

    description = [i[0] for i in res]
    rsid        = [i[1] for i in res]
    mutation    = [i[2] for i in res] 

    return jsonify({'Description' : description ,'Rsid' : rsid, 'Mutation' : mutation})

def get_basic_counters(dbfile):
    logging.info("Opening database file %s" % dbfile)
    con = sqlite3.connect(dbfile)
    cur = con.cursor()

    logging.debug("Reading data from database")
    cur.execute("SELECT DISTINCT t1.* FROM disease AS t1 JOIN disease_analyze AS t2 ON (t1.DISEASE_NAME = t2.DISEASE_NAME) ORDER BY 2 DESC")
    res = cur.fetchall()

    con.close()
    return (res)

def traits_get_basic_counters(dbfile):
    logging.info("Opening database file %s" % dbfile)
    con = sqlite3.connect(dbfile)
    cur = con.cursor()

    logging.debug("Reading data from database")
    cur.execute("SELECT DISTINCT t1.* FROM traits AS t1 JOIN traits_analyze AS t2 ON (t1.TRAIT_NAME = t2.TRAIT_NAME) ORDER BY 2 DESC")
    res = cur.fetchall()

    con.close()
    return (res)

def coronavirus_get_basic_counters(dbfile):
    logging.info("Opening database file %s" % dbfile)
    con = sqlite3.connect(dbfile)
    cur = con.cursor()

    logging.debug("Reading data from database")
    cur.execute("SELECT DISTINCT t1.* FROM coronavirus AS t1 JOIN coronavirus_analyze AS t2 ON (t1.CORONAVIRUS_NAME = t2.CORONAVIRUS_NAME) ORDER BY 2 DESC")
    res = cur.fetchall()

    con.close()
    return (res)
