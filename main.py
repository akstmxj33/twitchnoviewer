import gevent.monkey
gevent.monkey.patch_socket()
gevent.monkey.patch_ssl()
from gevent.pool import Pool
import requests, time, sys, re, base64
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', -1)

session = requests.session()
pool = Pool()

def append(e):
    livelist.append({'title':e['title'],
                     'viewer':e['viewer_count'],
                     'language':e['language'],
                     'game_id':e['game_id'],
                     'url':'https://www.twitch.tv/%s'%re.findall('_user_(.*?)-',e['thumbnail_url'])[0],
                     'image':e['thumbnail_url'].replace('{width}',str(num1)).replace('{height}',str(num2))})
                     #'image':'<img src="%s">'%e['thumbnail_url'].replace('{width}','320').replace('{height}','180')})
def getlive(cursor):
    if cursor == '':
        lives = 'https://api.twitch.tv/helix/streams?&first=100'
        liveget = session.get(lives,headers={'Client-ID': 'xsekjg7hdikenu62hnpkee5w91tbu9','Accept':'application/vnd.twitchtv.v5+json'})
        for e in (liveget.json()['data']): append(e)
        return getlive(liveget.json()['pagination']['cursor'])
    else:
        while True:
            lives = 'https://api.twitch.tv/helix/streams?&first=100&after=' + cursor
            liveget = session.get(lives,headers={'Client-ID': 'xsekjg7hdikenu62hnpkee5w91tbu9','Accept':'application/vnd.twitchtv.v5+json','Authorization':'Bearer a2up72bi327otj67kdqkmtg17m5lqz'}).json()
            try:
                if liveget['error'] == 'Too Many Requests':time.sleep(1);continue
            except:pass
            for e in (liveget['data']): append(e)
            if liveget['pagination'] == {}: return 'end'
            return liveget['pagination']['cursor']
        
def make_html(ch):
    ranks = [];counts = [];imgs = []
    html = BeautifulSoup(ch,'lxml');table = html.find('table');table['style'] = u'font-family:맑은 고딕;border-collapse:collapse;border: 1px solid transparent'
    thead = html.find('thead');tbody = html.find('tbody')
    for i, th in enumerate(tbody.find_all('th')):ranks.append(th)
    for i, td in enumerate(tbody.find_all('td')):
        if '_user_' in td.text:imgs.append(td)
        else:counts.append(td)
            
    html = '''<head><link rel="stylesheet" href="D:\database\Studieren\python\DCstatistics\dccon.css"></head><table align="center" cellspacing="0" cellpadding="0" border="0"style="u'font-family:맑은 고딕;border-collapse:collapse;border: 1px solid transparent'"><tbody></tbody></table>'''
    html = BeautifulSoup(html,'lxml')
    tbody = html.find('tbody')
    for i in range(0,int(len(imgs)/5)):
        firsttr = html.new_tag("tr");tbody.append(firsttr)
        for img in imgs[:5]:
            newtd = html.new_tag("td");firsttr.append(newtd)
            top = str(5)
            newtd['style'] = '''text-align: center; width: 100px;color: white;background-color:lightgray;border-top: 3px solid darkred;border-left: 3px solid darkred;border-right: 3px solid darkred;'''
            atag = html.new_tag("a",href=df[df['image']==img.text]['url'].values[0]);
            newimg = html.new_tag("img",  src=img.text);atag.append(newimg);newtd.append(atag);imgs.remove(img)
        secondtr = html.new_tag("tr");tbody.append(secondtr)
        for count in counts[:5]:
            newtd = html.new_tag("td");secondtr.append(newtd)
            newtd['style'] = '''text-align: center;color: white;background-color:black;border-left: 3px solid darkred;border-right: 3px solid darkred;border-bottom: 3px solid darkred;'''
            newimg = html.new_tag("h5a");newtd.append(newimg);newtd.h5a.string = count.text
            counts.remove(count)
    ch = str(html)
    return ch
    
z=1.25
num1 = int(320*z); num2 = int(180*z)
livelist = []
blacklist = ['488552','21779','493057']
language = ['ko','jp','de','rs']

for i in range(0,50):
    page = 30000+100*i; page2 = page+200
    cursor_d = '{"b":{"Offset":%s},"a":{"Offset":%s}}'%(page,page2)
    cursor = (base64.b64encode(cursor_d.encode('utf-8'))).decode('utf-8')[:-2]
    pool.spawn(getlive,cursor)
pool.join()


df = pd.DataFrame(livelist)
df = df[df['language'].isin(language)]
df = df[~df['game_id'].isin(blacklist)]
df = df
df = df.sort_values('viewer',ascending=True)
df = make_html(df.drop_duplicates()[['title','image']].to_html(escape=False))
with open('twitch.html','w',encoding='utf-8') as file:
    file.write(df)