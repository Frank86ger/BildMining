# -*- coding: utf-8 -*-
"""
Created on Fri Sep  8 14:36:49 2017

@author: Frank
"""

import feedparser as feedp
import BildArtcl as BA
import numpy as np
import time
import os
import datetime



tmppath = os.getcwd()+"\\latest_entry"
ID_MAX = 80
latest_entry = "NIX"
#np.savez(tmppath, latest_entry=latest_entry)

while True:
    ## Hier eine Schleife anfangen 'for ever'
    d = feedp.parse("http://www.bild.de/rssfeeds/vw-alles/vw-alles-26970192,sort=1,view=rss2,wtmc=ob.feed.bild.xml")
    latest_entry = str(np.load(tmppath+'.npz')['latest_entry'])
    # alle links / id der ersten 80 eintraege
    ID_OLD = ID_MAX
    ids = []
    for id in range(0,ID_MAX):
        ids.append(d.entries[id]['id'])
        if (d.entries[id]['id'] == latest_entry):
            ID_OLD = id
    # ID_OLD = 80: 79->0
    # ID_OLD = 0: garnix
    # ID_OLD = x: x-1 -> 0
    
    if (ID_OLD!=0):
        for id in reversed(range(0, ID_OLD)):
            print(str(id).zfill(2)+ " ::: " + str(datetime.datetime.now()))
            try:
                MyArticle = BA.Article(d.entries[id])
                MyArticle.GetArticle()
                MyArticle.SaveArticle()
                #SaveArticle(d,id)
            except:
                pass
    
            time.sleep(1)
            latest_entry = d.entries[0]['id']
            
    np.savez(tmppath, latest_entry=latest_entry)
    time.sleep(1800) # jede halbe stunde abrufen
    
