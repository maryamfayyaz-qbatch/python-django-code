from threading import *		
import time		
import http.client
import threading
import time
import json
import csv
import mws
import mysql.connector


UPCS = ['010343957879', '010343947962', '010343901407', '885131620477', '769622735102', '885131626905', '885131624574', '859443003372', '872984003120', '663370120022', '074000611122', '732109400053']
mws_accounts = []
sem = threading.Semaphore()

def get_amazon_accounts():
  conn = http.client.HTTPSConnection("app.ecomcircles.com")
  payload = ''
  headers = {
    'Authorization': 'Basic bWF0Y2hpbmdfc3lzdGVtOnRLMFZrTHRB\\n'
  }
  conn.request("GET", "/api/marketplace_accounts/fetch_all?marketplace=amazon", payload, headers)
  res = conn.getresponse()
  data = res.read()
  json_response = json.loads(data)
  for accounts in json_response['data']:
    mws_accounts.append([accounts['seller_id'], accounts['mws_token']])
  

def get_upc():
  sem.acquire()
  if (len(UPCS) ) : upc = UPCS.pop()
  sem.release()	
  return upc

def get_account():
  sem.acquire()
  if ( len(mws_accounts) ) : account = mws_accounts.pop()
  sem.release()
  return account

def get_matching_product():
  upc = get_upc()
  for i in range(10):
      try:
        account = get_account()
        print (account)
        products_api = mws.Products(
            access_key = "AKIAI5UG2DBY3HDNFTUA",
            secret_key = "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",
            account_id = account[0],
            auth_token = account[1],
        )
        resp = products_api.get_matching_product_for_id(
            'ATVPDKIKX0DER',
            "UPC",
            upc,
        )
        if resp.response.status_code == 200: break
          
      except Exception as e:
        print ("Error alert", e)
  print (resp.parsed)
  save_data(resp.parsed)

def save_data(response):
  try:
    response_body = response['Products']['Product'][0]['AttributeSets']['ItemAttributes']['PackageDimensions']
    length =  round(float(response_body['Length']['value']),2)
    width =  round(float(response_body['Width']['value']),2)
    height =  round(float(response_body['Height']['value']),2)
    weight =  round(float(response_body['Weight']['value']),2)
    cnx = mysql.connector.connect(user='root', password='password',
                                host='localhost', database='pythonTest',
                                auth_plugin='mysql_native_password')

    mycursor = cnx.cursor()
    sql = "INSERT INTO dimensions (length, width, height, weight) VALUES (%s, %s, %s, %s)"
    val = (length, width, height, weight)
    mycursor.execute(sql, val)
    cnx.commit()
  except Exception as e:
    print ("Error alert", e)  
           

threads = []
get_amazon_accounts()
for i in range(len(UPCS)):
  t1 = Thread(target = get_matching_product )
  threads.append(t1)
for thread in threads:
  thread.start()
  thread.join()
