import json
from typing import Text
import pymysql
import json
import pandas as pd
from pandas import DataFrame
import PyPDF2 
import sys
from google.cloud import storage 
from google.cloud import vision
import re
import os 
import numpy as np
from datetime import datetime
d = ['fir_no','fir_content','fir_report']
df = pd.DataFrame(columns=d)
path = 'D:\\CCTNS1\\pdf'

os.chdir(path)

psid = str(input('Enter the police station id '))

sid = psid[:2]
path = os.path.join(path,sid)
did = psid[:-3]
path = os.path.join(path,did)
p= os.path.join(path,psid)

os.chdir(p)
l = os.listdir()




def check_sql(x):
    con = pymysql.connect(host = 'localhost',user = 'root',passwd = 'Firfile123',db = 'fir_table1')
    cursor = con.cursor()
    #SELECT EXISTS(SELECT * from ExistsRowDemo WHERE ExistId=105)
    #INSERT INTO FIR(sl_n,book_no,FIR_CONTENTS ) VALUES (%s,	%s,	%s)
    cursor.execute("SELECT EXISTS (SELECT * FROM regis WHERE FIR_REG_NUM=(%s))", (int(x)))
    rows = cursor.fetchall()
    con.commit()
    con.close()
    if rows[0][0] == 0:
        return 1
    else :
        return 0
    
f_l = []
for i in l:
   
    fir_no = int(i[:-4])
    if(check_sql(fir_no)):
         f_l.append(i)
   
    

s =[]
fa = []

path = 'D:\\CCTNS1\\fir.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path
storage_client = storage.Client.from_service_account_json(path)


# Creating bucket object

bucket = storage_client.get_bucket('fir_files')

for i in f_l:
        batch_size = 40
        mime_type = 'application/pdf'
        path = 'D:\\CCTNS1\\fir.json'
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path
        storage_client = storage.Client.from_service_account_json(path)
        filename = i
        fir_no = int(i[:-4])
        # Name of the object to be stored in the bucket
        object_name_in_gcs_bucket = bucket.blob(filename)

        # Name of the object in local file system

        object_name_in_gcs_bucket.upload_from_filename(filename)
        client = vision.ImageAnnotatorClient()

        feature = vision.Feature(
            type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION)

        gcs_source_uri = 'gs://fir_files/' + filename
        gcs_source = vision.GcsSource(uri=gcs_source_uri)
        input_config = vision.InputConfig(
            gcs_source=gcs_source, mime_type=mime_type)

        gcs_destination_uri = 'gs://fir_files/'+ filename + 'idkwhybutplease'
        gcs_destination = vision.GcsDestination(uri=gcs_destination_uri)
        output_config = vision.OutputConfig(
            gcs_destination=gcs_destination, batch_size=batch_size)

        async_request = vision.AsyncAnnotateFileRequest(
            features=[feature], input_config=input_config,
            output_config=output_config)

        operation = client.async_batch_annotate_files(
            requests=[async_request])

        print('Waiting for the operation to finish.')
        operation.result(timeout=180)



        match = re.match(r'gs://([^/]+)/(.+)', gcs_destination_uri)
        bucket_name = match.group(1)
        prefix = match.group(2)
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)


        blob_list = list(bucket.list_blobs(prefix=prefix))

        fir_text = ""
        a=[]
        now = datetime.now() # current date and time
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")

        output = blob_list[0]

        json_string = output.download_as_string()

        response = json.loads(json_string)
        try:
            for i in response['responses']:
                fir_text = fir_text + i['fullTextAnnotation']['text']
                
                for page in i['fullTextAnnotation']['pages']:
                    for block in page['blocks']:
                        for paragraph in block['paragraphs']:
                            for word in paragraph['words']:
                                word_text = ''.join([
                                    symbol['text'] for symbol in word['symbols']
                                ])
                                a.append(word['confidence'])

            percent = np.mean(a) *100
            confidence = round(percent,1)
            conf = int(confidence)
            x1 = response['responses'][0]
            x2 = response['responses'][1]
            x3 = response['responses'][2]
            x4 = response['responses'][3]
            ft = len(response['responses'])
            #x9 = response['responses'][ft-5]
            #x10 = response['responses'][ft-4]
            #x11 = response['responses'][ft-3]
           # x12= response['responses'][ft-2]
            #x13= response['responses'][ft-1]
            c = response['responses'][-3:]
            comments = ""

            for i in c:
                comments = comments +i['fullTextAnnotation']['text']

            
        
            fir_cont =   x2['fullTextAnnotation']['text']+x3['fullTextAnnotation']['text']+x4['fullTextAnnotation']['text']
            fir_text = fir_text.replace('\n','')
           # comments = x9['fullTextAnnotation']['text'] + x10['fullTextAnnotation']['text']+x11['fullTextAnnotation']['text']+x12['fullTextAnnotation']['text']+x13['fullTextAnnotation']['text']
           # comments=comments.replace("\n","")
            fir_cont = fir_cont.replace("\n",'')

        
            t1 = re.search('if required(.*)Action taken',fir_text)
            t2 = re.search('First Information contents(.*)Action taken',fir_text)
            t3 = re.search('First Information contents(.*)13',fir_text)
            t4 = re.search('contents(.*)13',fir_text)
            t5 = re.search('contents(.*)Action taken',fir_text)
            t6 = re.search('contents(.*)taken',fir_text)
            t7 = re.search('F.I.R contents(.*)taken',fir_text)
            if (t1):
                t = t1.group(0)
            elif(t2):
                t = t2.group(0)
            elif(t3):
                t = t3.group(0)
            elif(t4):
                t = t4.group(0)
            
            elif(t5):
                t = t5.group(0)
            
            elif(t6):
                t= t6.group(0)
                
            elif(t7):
                t= t7.group(0)
            else:
                t = fir_cont
            comments = ""
            for i in response['responses'][-3:]:
                comments = comments + i['fullTextAnnotation']['text']
            comments = comments.replace('\n','')
            u1= re.search('Status of the accused(.*)चोट प्रतीवेदन',fir_text)
            u2 = re.search('Status of the accused(.*)',fir_text)
            u10 = re.search('FORM / REPORT(.*)',fir_text)
            u3 = re.search('FINAL FORM / REPORT(.*)',fir_text)
            u4 = re.search('FINAL FORM(.*)',fir_text)
            u5 = re.search('अंतिम फॉर्म(.*)',fir_text)
            u6 = re.search('अंतिम परिणाम(.*)',fir_text)
            u7 = re.search('अंतिम-परिणाम(.*)',fir_text)
            u = re.search('Brief facts of the case(.*)',fir_text)
            u8 = re.search('I.I.F.-V(.*)',fir_text)
            u9 = re.search('एकीकृत जाचँ फॉर्म-V(.*)',fir_text)
            

            if(u):
                comm = u.group(0)
                
            elif(u1):
                comm = u1.group(0)
               
            elif(u2):
                comm = u2.group(0)
                
            elif(u3):
                comm = u3.group(0)
                
            elif(u4):
                comm = u4.group(0)
               
            elif(u5):
                comm = u5.group(0)
                
            elif(u6):
                comm = u6.group(0)
               
            elif(u7):
                comm = u7.group(0)
              
            elif(u8):
                comm = u8.group(0)
               
            elif(u9):
                comm = u9.group(0)
               
            elif(u10):
                comm = u10.group(0)
               

            else:
                comm = comments

                #print('error in I.I.F-V content extraction')
                # conf = - conf
                #comm = 'I.I.F. - V content could not be extracted'
                #con = pymysql.connect(host = 'localhost',user = 'root',passwd = 'Firfile123',db = 'fir_table1')
               # cursor = con.cursor()
                #cursor.execute("INSERT INTO regis(FIR_REG_NUM,FIR_CONTENTS,COMMENTS,confidence,timeofextraction) VALUES (%s,%s,%s,%s,%s)", (int(fir_no),t,comm,int(conf),date_time))
               # cursor.execute("INSERT INTO success1(FIR_REG_NUM,FIR_CONTENTS,COMMENTS,confidence,timeofextraction) VALUES (%s,%s,%s,%s,%s)", (int(fir_no),t,comm,int(conf),date_time))
               # con.commit()
            con = pymysql.connect(host = 'localhost',user = 'root',passwd = 'Firfile123',db = 'fir_table1')
            cursor = con.cursor()
            cursor.execute("INSERT INTO regis(FIR_REG_NUM,FIR_CONTENTS,COMMENTS,confidence,timeofextraction) VALUES (%s,%s,%s,%s,%s)", (int(fir_no),t,comm,int(conf),date_time))
            cursor.execute("INSERT INTO success1(FIR_REG_NUM,FIR_CONTENTS,COMMENTS,confidence,timeofextraction) VALUES (%s,%s,%s,%s,%s)", (int(fir_no),t,comm,int(conf),date_time))
            con.commit()
            con.close()   # con.close()
            print("File with fir number  {} has been extracted".format(fir_no))
           
            s.append(filename)
        except:


            print("Error in extracting file {} ".format(fir_no))
            con = pymysql.connect(host = 'localhost',user = 'root',passwd = 'Firfile123',db = 'fir_table1')
            cursor = con.cursor()
            cursor.execute("INSERT INTO regis(FIR_REG_NUM) VALUES (%s)", (int(fir_no)))
            cursor.execute("INSERT INTO rejection(FIR_REG_NUM) VALUES (%s)", (int(fir_no)))
            con.commit()
            con.close()
            fa.append(filename)
print("files extracted successfully are \n")
print(s)
print("Files with error in extraction")
print(fa)


