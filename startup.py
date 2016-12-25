# Copyrighted by Sean Lydon and licensed under GPLv3

# Typically I have this file as ~/.startup.py and in my ~/.bashrc I have
# export PYTHONSTARTUP=~/.startup.py
# to always preload these methods in my python REPL.

import json
from pprint import pprint as pprint_fn

pp = lambda x: pprint_fn(x, width=200)

try:
  import readline
except ImportError:
  print("Module readline not available.")
else:
  import rlcompleter
  readline.parse_and_bind("tab: complete")
    
def dist(d):
  r = {}
  for x in d:
    r[x] = r.get(x, 0) + 1
  return r

sg = lambda x, y: x.get(y, {})

def loadalldata(f):
  skipped = 0
  data = []
  for x in open(f, 'r'):
    if x[0] == '{' and x[-1] == '\n':
      data.append(json.loads(x))
    else:
      skipped += 1
  print 'Skipped %d' % (skipped,)
  return data

def loaddata(f, fn=lambda x: True):
  skipped = 0
  for x in open(f, 'r'):
    if x[0] == '{' and x[-1] == '\n':
      d = json.loads(x)
      if fn(d):
        yield d
      else:
        skipped += 1
    else:
      skipped += 1
  print 'Skipped %d' % (skipped,)

def readX(g, x):
  data = []
  for i, d in enumerate(g):
    if i >= x:
      return data
    data.append(d)
  return data

dumps = lambda x: json.dumps(x, separators=(",",":"))

commonkeys = lambda d: reduce(lambda a, b: a & b, map(lambda x: set(x.keys()), d))

def distkeys(d):
  c = commonkeys(d)
  return dist([str(sorted(set(x.keys()) - c)) for x in d])

def groupKey(d, fn):
  res = {}
  for x in d:
    k = fn(x)
    if k not in res:
      res[k] = [x]
    else:
      res[k].append(x)
  return res

def savehistory(fn):
  import readline
  fp = open(fn, 'w')
  for i in xrange(readline.get_current_history_length()+2):
    l = readline.get_history_item(i)
    if l is not None:
      fp.write(l + '\n')
  fp.close()

from datetime import datetime

def utc_now():
  return (datetime.now() + timedelta(hours=7)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

def deserializePythonTime(t):
  d = datetime.strptime(t, '%Y-%m-%dT%H:%M:%S.%fZ')
  return int(totimestamp(d) * 1000)

def deserializeJodaTime(t):
  d = datetime.strptime(t, '%Y-%m-%dT%H:%M:%SZ')
  return int(totimestamp(d) * 1000)

def totimestamp(dt, epoch=datetime(1970,1,1)):
  td = dt - epoch
  # Doesn't work on windows: return td.total_seconds()
  return (td.microseconds + (td.seconds + td.days * 86400) * 10**6) / 10**6
