#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 10 09:31:37 2020

@author: Leilah Harouni
"""
from facepplib import FacePP
import requests
import json
import pandas as pd
from pathlib import Path
from csv import writer
import csv


# Working directory
directory = str(Path.home()) + "/Dropbox/Twitter_Image_Scrape/"

# Face++ API key & secret
api_key = 'G91YUGXgRwtj_zaFU84e1dbszZvkdYvM'
api_secret = 'pLE1YqkzHIwR0tPuEE0VCWMwzvOMj5_l'


#####
json_resp = requests.post(
    'https://api-us.faceplusplus.com/facepp/v3/detect',
    data = {
        'api_key'    : api_key,
        'api_secret' : api_secret,
        'image_url'  : 'http://pbs.twimg.com/profile_images/673245801209032704/YmsTbMZQ.jpg'
    }
)

node = json.loads(json_resp.text)
face_token = node['faces'][0]['face_token']

json_resp2 = requests.post(
    'https://api-us.faceplusplus.com/facepp/v3/face/analyze',
    data  = {
        'api_key'           : api_key,
        'api_secret'        : api_secret,
        'face_tokens'       : face_token,
        'return_landmark'   : 0,
        'return_attributes' : ['gender']
    }
)

print("Response2 : ", json_resp2.text)



csvFile = open('TwitterFacePP.csv', 'a')
csvWriter = csv.writer(csvFile)

for data in json_resp2:
    csvWriter.writerow(['return_attributes'])
print ('return_attributes')
   
csvFile.close()
