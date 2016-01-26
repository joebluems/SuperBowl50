import sys
import re
import urllib2
filename = sys.argv[1]
season = filename[-4:]
count=0

html_prefix ="http://www.pro-football-reference.com/boxscores/"

#### read season file - extract URL for each game ####
with open(filename) as f:
    lines = f.readlines()
    for a in lines:
      #if count==0:
      if re.search(r'boxscore',a) and re.search(r'htm',a):
	url = html_prefix + re.sub(r"htm.*$","htm",re.sub(r".*/boxscores/","",a.strip()))
	count=1
	#### extract information from the game url ####
	title = ''
	time = ''
	roof = ''
	surface = ''
	weather = ''
	vegas = ''
        over_under = ''
	result = ''
	flag=0
	try:
	  response=urllib2.urlopen(url)
	  html = response.read()
	  for l in html.split("\n"):
   	    if flag==1: 
		time = re.sub(r"^.*>","",re.sub(r"</td>","",l))
		flag = 0
 	    if flag==2: 
		roof = re.sub(r"^.*>","",re.sub(r"</td>","",l))
		flag = 0
 	    if flag==3: 
		surface = re.sub(r"^.*>","",re.sub(r"</td>","",l))
		flag = 0
 	    if flag==4: 
		weather = re.sub(r"^.*>","",re.sub(r"</td>","",l))
		flag = 0
 	    if flag==5: 
		vegas = re.sub(r"^.*>","",re.sub(r"</a></td>","",l))
		flag = 0
 	    if flag==6: 
		over_under = re.sub(r"<.*$","",re.sub(r'^.*align="" >',"",l))
		flag = 0

	    if re.match(r'<!DOCTYPE html>',l): title = re.sub(r"^.*<title>","",re.sub(r"</title>.*$","",l))
	    ##if re.match(r'<h1',l): result = re.sub(r"^.*>","",re.sub(r"</h1>.*$","",l))
	    if re.match(r'<h1',l): result = re.sub(r"^.*>","",re.sub(r"</h1>$","",l))
	    if re.search(r'>Start Time',l): flag=1
	    if re.search(r'>Roof<',l): flag=2
	    if re.search(r'>Surface<',l): flag=3
	    if re.search(r'>Weather<',l): flag=4
	    if re.search(r'>Vegas Line<',l): flag=5
	    if re.search(r'>Over/Under<',l): flag=6
	except: print "ERROR with URL"

        title = re.sub(r"\|.*$","",title)
	total = 0
	x = [int(s) for s in result.split() if s.isdigit()]
	for a in x: total+=a


	print title + "|" + time + "|" + roof + "|" + surface + "|" + weather + "|" + vegas + "|" + over_under + "|" + str(total) + "|" + season

