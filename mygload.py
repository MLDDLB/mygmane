import sqlite3
import time
import urllib.request, urllib.error, urllib.parse
import ssl
import re

def makestarturl(baseurl, start):
    if start is not None:
        return baseurl + str(start) + '/' + str(start + 1)
    else:
        print("bad url")
        return none

conn = sqlite3.connect('emails.sqlite')
cur = conn.cursor()

#certificate
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

baseurl = "http://mbox.dr-chuck.net/sakai.devel/"

cur.execute('''CREATE TABLE IF NOT EXISTS Emails
(id INTEGER PRIMARY KEY UNIQUE,
from_ad TEXT,
headers TEXT,
body TEXT)''')

#Pick up where we left off
start = None
cur.execute('SELECT max(id) FROM Emails')
row = cur.fetchone()
if row[0] is None:
    start = 0
else:
    print("max id =", row[0])
    start = row[0]

many = 0
fail = 0
while True:
    if many < 1:
        conn.commit()
        try:
            sval = input("How many messages to retrieve?")
            if len(sval) < 1 or sval == 'break':
                break
            many = int(sval)
        except:
            print("Bad data! Try again")
            many = 0
            continue
    start = start + 1

    try:
        url = makestarturl(baseurl, start)
        uhandle = urllib.request.urlopen(url, None, 30, context = ctx)
        document = uhandle.read().decode()
        if uhandle.getcode() != 200:
            print('Error code:', uhandle.getcode(), url)
            break
    except KeyboardInterrupt:
        print("Interrupted by user")
        break
    except Exception as e:
        print("Unable to retrieve or parse page", url)
        print("Error", e)
        fail = fail + 1
        if fail > 5: break
        continue

    print('Retrieved', url, len(document))
    many = many - 1

    if not document.startswith("From "):
        print('Bad data!')
        fail = fail + 1
        if fail > 5: break
        continue

    id = start
    fromwho = None
    buf = re.findall("From: .* <(\S+@\S+)>", document)
    if len(buf) == 1:
        fromwho = buf[0]
        fromwho = fromwho.strip().lower()
        fromwho = fromwho.replace('<', ' ')
    else:
        buf = re.findall("\nFrom: (\S+@\S+)\n", document)
        if len(buf) == 1:
            fromwho = buf[0]
            fromwho = fromwho.strip().lower()
            fromwho = fromwho.replace('<', ' ')


    #fromwho = document.split()[1]

    pos = document.find("\n\n")
    if pos > 0:
        headers = document[:pos]
        body = document[pos+2:]
    else:
        print("Could not find the break between headers and the body")
        fail = fail + 1
        if fail > 5: break
        continue

    cur.execute('''INSERT INTO Emails(id, from_ad, headers, body)
    VALUES (?, ?, ?, ?)''', (id, fromwho, headers, body))

    if many%50 == 0: conn.commit()
    if many%100 == 0: time.sleep(1)
