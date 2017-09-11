# -*- coding: utf-8 -*-
"""
Created on Thu Sep  7 14:31:55 2017

@author: Frank
"""

class Article(object):
    """ Article docstring """
    
    # Imports
    import re
    import feedparser as fp # brauch ich das ?
    import datetime
    import urllib3
    from bs4 import BeautifulSoup
    import json
    import psycopg2
    from psycopg2 import sql
    
    def __init__(self, rss_entry):
        """ INIT """
        self.RSS_Entry = rss_entry # feedparser.FeedParserDict type
        self.TheArticle = [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None] # list of all the entries

    def CleanString(self, mystring):
        """ Clean the String """
        mystring = bytes(mystring, 'utf-8').decode('utf-8', 'ignore') # remove non-utf-8 chars
        mystring = self.re.sub("'", '"', mystring, self.re.UNICODE) # replace all ' with "
        mystring = self.re.sub('"""', '"', mystring, self.re.UNICODE) # replace all """ by "
        return mystring

    def StrToDatetime(self, date_str):
        """ Convert String to Datetime object """
        month_dic = {'Jan' : 1, 'Feb' : 2, 'Mar' : 3, 'Apr' : 4, 'May' : 5, 'Jun' : 6, 'Jul' : 7, 'Aug' : 8, 'Sep' : 9, 'Oct' : 10, 'Nov' : 11, 'Dec' : 12}
        return self.datetime.datetime(int(date_str[12:16]), month_dic[date_str[8:11]], int(date_str[5:7]), int(date_str[17:19]), int(date_str[20:22]), int(date_str[23:25]))

    def ReadPageChans(self, soup):
        """ 123 """
        page_chans = self.re.findall(r'"page_channel.*', soup[1].text, self.re.MULTILINE)
        _, _, keyw1 = page_chans[0].partition(": ")
        _, _, keyw2 = page_chans[1].partition(": ")
        _, _, keyw3 = page_chans[2].partition(": ")
        return [keyw1[1:-2], keyw2[1:-2], keyw3[1:-2]]

    def GetArticle(self):
        """ Docstring: Get the Article """
        
        #--rss article title
        rss_title = self.RSS_Entry['title']
        self.TheArticle[0] = self.CleanString(rss_title)
        
        #--rss article summary
        rss_summary = str(self.RSS_Entry['summary_detail']['value']) ## muss ich das str casten oder ist es schon str ?
        rss_summary = self.re.search('\n\n(.*?)<br', rss_summary).group(1)
        self.TheArticle[1] = self.CleanString(rss_summary)
        
        #--rss article publication date
        rss_date = self.RSS_Entry['published']
        self.TheArticle[2] = self.StrToDatetime(rss_date)

        #--rss Day of the week
        self.TheArticle[3] = rss_date[0:3]

        #--rss article id/link # -->> USE AS UNIQUE ID <<--
        rss_link = self.RSS_Entry['id']
        self.TheArticle[4] = str(rss_link)

        #--rss article TAGS
        rss_tags = []
        for cats in (self.RSS_Entry['tags'][:]):
            rss_tags.append(cats['term'])
        self.TheArticle[5] = rss_tags   
        
        #----------------------------------        
        #--get data from article source code
        http = self.urllib3.PoolManager()
        url = self.RSS_Entry['id']
        response = http.request('GET', url)
        soup = self.BeautifulSoup(response.data, "lxml")

        #--json data ld+json
        json_data = self.json.loads(soup.find('script', type='application/ld+json').text)
        
        #-- publication type
        json_type = json_data['@type']
        self.TheArticle[6] = self.CleanString(json_type)
        
        #-- keywords
        json_keywords = json_data['keywords']
        self.TheArticle[7] = self.CleanString(json_keywords)

        #-- author
        json_author_type = json_data['author']['@type']
        json_author_name = json_data['author']['name']
        self.TheArticle[8] = self.CleanString(json_author_type)
        self.TheArticle[9] = self.CleanString(json_author_name)
        
        #-- publisher
        json_publisher_type = json_data['publisher']['@type']
        json_publisher_name = json_data['publisher']['name']
        self.TheArticle[10] = self.CleanString(json_publisher_type)
        self.TheArticle[11] = self.CleanString(json_publisher_name)

        #-- page channels from js
        js_text = soup.find_all('script', type="text/javascript")
        self.TheArticle[12] = self.ReadPageChans(js_text)

        #------------------------------------
        #-- picture captions and article text
        
        #  -- ALLE Bilder Texte --
        # alles in '<figcaption> ... </figcaption>'
        pic_captions = []
        pic_captions_tmp = soup.findAll('figcaption')
        for pic_caption in pic_captions_tmp:
            pic_captions.append(pic_caption.get_text().strip())
        pic_captions = ';'.join(pic_captions)
        self.TheArticle[13] = self.CleanString(pic_captions)


        # -- Fliess Text --
        # concat alles in '<p> ... </p>' fuer TEXT, getrennt durch \n
        # <strong> (boldface) entfernen ?
        the_text = []
        text_content = soup.find_all('div', class_="txt")[0]
        text_parts = text_content.findAll('p')
        for text_part in text_parts:
            the_text.append(text_part.get_text())
        the_text = ''.join(the_text)
        self.TheArticle[14] = self.CleanString(the_text)
        
    
    def CreateTable(self, table_name):
        """ 123 """
        
        conn = self.psycopg2.connect("dbname=Bild_Mine user=postgres password=pc")
        cur = conn.cursor()
        
        cmd = """
            CREATE TABLE """+ table_name +""" (
            article_id SERIAL PRIMARY KEY,
            rss_title VARCHAR(8000),
            rss_summary VARCHAR(8000),
            rss_date TIMESTAMP,
            rss_day VARCHAR(3),
            rss_link VARCHAR(8000),
            rss_tags VARCHAR(8000)[],
            json_type VARCHAR(8000),
            json_keywords VARCHAR(8000),
            json_author_type VARCHAR(8000),
            json_author_name VARCHAR(8000),
            json_pub_type VARCHAR(8000),
            json_pub_name VARCHAR(8000),
            js_page_channels VARCHAR(8000)[],
            pic_captions VARCHAR(8000),
            text_content VARCHAR(8000));
            """
        cur.execute(cmd)
        cur.close()
        conn.commit()
        conn.close()
        print(" ===> Table created: " + table_name)


    def SaveArticle(self):
        """ 123 """
        rss_date = self.RSS_Entry['published']
        month_dic = {'Jan' : 1, 'Feb' : 2, 'Mar' : 3, 'Apr' : 4, 'May' : 5, 'Jun' : 6, 'Jul' : 7, 'Aug' : 8, 'Sep' : 9, 'Oct' : 10, 'Nov' : 11, 'Dec' : 12}
        #table_name = 'bild_'+str(month_dic[rss_date[8:11]]).zfill(2)+"_"+rss_date[12:16]
        table_name = 'bild'+str(month_dic[rss_date[8:11]]).zfill(2)+rss_date[12:16]


        ###---
        conn = self.psycopg2.connect("dbname=Bild_Mine user=postgres password=pc")
        cur = conn.cursor()
        
        cmd = """SELECT EXISTS (
            SELECT 1
            FROM   information_schema.tables 
            WHERE  table_schema = 'public'
            AND    table_name = '"""+table_name+"""'
            );
            """
        cur.execute(cmd)
        table_exists = cur.fetchone()[0]
        cur.close()
        conn.commit()
        conn.close()
        ###---

        if(not table_exists):
            self.CreateTable(table_name)



        #-- INSERT VALUES INTO TABLE
        conn = self.psycopg2.connect("dbname=Bild_Mine user=postgres password=pc")
        cur = conn.cursor()
        ####################

        cmd = """INSERT INTO %s (rss_title, rss_summary, rss_date, rss_day, rss_link, rss_tags, json_type, json_keywords, json_author_type, json_author_name, json_pub_type, json_pub_name, js_page_channels, pic_captions, text_content)
                VALUES (%%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s);""" # Note: no quotes
        
        mytuple = tuple(self.TheArticle)

        cur.execute(cmd % table_name, mytuple)
        cur.close()
        conn.commit()
        conn.close()
        print("Article saved in "+table_name+" ==> "+self.TheArticle[0])
        
