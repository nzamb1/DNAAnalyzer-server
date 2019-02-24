import subprocess
import sqlite3
import logging
from flask import jsonify, Flask
from flask import request
app = Flask(__name__)


logging.basicConfig(filename="server.log", format = u'LINE:%(lineno)d# %(levelname)s %(asctime)s %(message)s', level = logging.INFO)

@app.route('/develfile',methods=['GET','POST'])
def develfile():
    if request.method == 'POST':
        username = request.form['userName']
        password = request.form['password']
        userfile = username + ".dat"
        print username
        print password
        print "Size of data:" + str(len(request.form['rawdata']))

        #import pdb
        #pdb.set_trace()
        with open(userfile, 'w') as file:
                file.write(request.form['rawdata'].encode("UTF-8"))
        print "File saved."

        subprocess.call(['python', 'process_file.py', '-f%s' % userfile, '-u%s' % username])
        return "ok"
    else:
        return "Something is wrong..."

@app.route("/basiccounters", methods=['POST', 'GET'])
def basiccounters():
    if request.method == 'POST':
        username = request.form['userName']
        logging.info("Username %s" % username)
        counters = get_basic_counters(username,'rs429358')
        logging.info("COunters %s" % str(counters))
        return jsonify({username : counters[0], 'Alzheimer' : counters[1]})
        #return jsonify(str(username)=counters[0],'Alzheimer'=counters[1])


def get_basic_counters(username,snp_id):
    logging.info("Opening database file %s" % "HARDCODED")
    con = sqlite3.connect('nzamb.lite')
    cur = con.cursor()

    logging.debug("Reading data from database")
    cur.execute("SELECT COUNT(*) FROM %s" % username)
    total_snps = cur.fetchall()[0][0]
    cur.execute("SELECT COUNT(*) FROM %s WHERE ID = '%s';" % (username, snp_id))
    snp_count_byid = cur.fetchall()[0][0]


    con.close()
    return (total_snps,snp_count_byid)
