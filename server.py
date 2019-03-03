import subprocess
import sqlite3
import logging
import base64
from flask import jsonify, Flask
from flask import request
from flask import abort
from flask import Response
app = Flask(__name__)


logging.basicConfig(filename="server.log", format = u'LINE:%(lineno)d# %(levelname)s %(asctime)s %(message)s', level = logging.DEBUG)

@app.route('/develfile',methods=['GET','POST'])
def develfile():
    if request.method == 'POST':
        username = request.form['userName']
        password = request.form['password']
        userfile = username + ".dat"
        logging.debug("Request recived username %s" % username)
        logging.debug("Request recived password %s" % password)
        logging.debug("Size of data:" + str(len(request.form['rawdata'])))

        with open(userfile, 'w') as file:
                file.write(request.form['rawdata'].encode("UTF-8"))
        logging.info("Raw file saved")
        print "File saved."

        retcode =  subprocess.call(['python', 'process_file.py', '-f%s' % userfile, '-u%s' % username])
        if (retcode <> 0):
            logging.error("Error processing RAW data file")
            abort(Response('Error processing file'))
        logging.info("Raw file processed successfully")
        return "ok"
    else:
        return "Something is wrong..."

@app.route("/basiccounters", methods=['POST', 'GET'])
def basiccounters():
    if request.method == 'POST':
        username = request.form['userName']
        logging.info("Username %s" % username)
        logging.info("Calling function to read data from DB")
        counters = get_basic_counters(username,'rs429358')

        disease     = [i[0] for i in counters]
        porbability = [i[1] for i in counters]
        icons       = [base64.b64encode(i[2]) for i in counters]

        return jsonify({'Porbability' : porbability ,'Disease' : disease, 'Icons' : icons})


def get_basic_counters(username,snp_id):
    logging.info("Opening database file %s" % "HARDCODED")
    con = sqlite3.connect('nzamb.lite')
    cur = con.cursor()

    logging.debug("Reading data from database")
    cur.execute("SELECT * FROM %s" % 'nzamb1_disease')
    res = cur.fetchall()

    con.close()
    return (res)
