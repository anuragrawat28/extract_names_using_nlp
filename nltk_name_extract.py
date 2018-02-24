# -*- coding: utf-8 -*-
import pandas as pd
import re
import sys
import requests
import json
import httplib
import collections
import nltk
from pymongo import MongoClient
from itertools import chain
from itertools import product
from nltk.corpus import stopwords
from nltk.tag import StanfordNERTagger
from nltk.tokenize import word_tokenize
reload(sys)
sys.setdefaultencoding('utf-8')
client=MongoClient('localhost',27000)
db1=client.digvijay
db=client.TCR
collection=db.name_db

documents=collection.find({},{'_id':0})

def names_to_companies():
	count=0
	collection = db.companies_data
	documents = collection.find({},{'_id':0})
	for document in documents:
		count+=1
		data = {'id' : '' ,'name' : ''}
		print count
		data['id'] = document['id'].encode('utf-8','strict').strip()
		data['name'] = document['name'].encode('utf-8','strict').replace(':','').strip()
		data['about'] = ' '.join(document['about'].encode('utf-8','strict').replace('\t',' ').replace('\\t',' ').replace('\r',' ').replace('\\r',' ').replace('\s+',' ').replace('!',' ').replace('/',' ').replace('\n',' ').replace('\\n',' ').strip().split())
		print data
		#db.companies_data.update_one({'id': data['id']}, {"$set": data})

def name_extraction():
	st = StanfordNERTagger('/home/anubha/Downloads/stanford/stanford-ner-2017-06-09/classifiers/english.all.3class.distsim.crf.ser.gz','/home/anubha/Downloads/stanford/stanford-ner-2017-06-09/stanford-ner.jar',encoding='utf-8')
	stop_words = set(stopwords.words('english'))

	count = 0
	for document in documents:
		count+=1
		#name_list = {'id' : '','contact_person' : {'name' : []}}
		name_list = {'id' : '','name' : [] }
		print count
		print document['url']
		name_list['id'] = document['id'].encode('utf-8').strip()
		name_list['url'] = document['url'].encode('utf-8').strip()
		for section in document['section']:
			tokenized_text = word_tokenize(section['data'])
			filtered_sentence = [w.encode('utf-8','strict') for w in tokenized_text if w not in stop_words] #stop words removal
			classified_text = st.tag(filtered_sentence)
			name_list['name'].append([classified[0].lower().encode('utf-8','strict') for classified in classified_text if classified[1] in ['PERSON']])
		name_list['name'] = list(set(chain(*(name_list['name']))))
		print name_list
		#db.name_db.insert(name_list)
		documents.close()
		#db.companies_data.update_one({'id': name_list['id']}, {"$set": {'contact_person.name' : name_list['contact_person']['name']}})

#names = name_extraction()

def put_name():
	collection=db.companies_data.find({},{'_id':0})
	collection1=db.name_db.find({},{'_id':0})
	count=0
	for document in collection:
		count+=1
		print count
		actual_name = {'id' : '','contact_person' : {'name' : []}}
		actual_name['id'] = document['id'].encode('utf-8').strip()
		if len(document['contact_person']['email']) > 0:
			print document['url']
			document['contact_person']['email'] = [x.lower() for x in document['contact_person']['email'] ]
			for name in document['contact_person']['name']:
				for email in document['contact_person']['email']:
					if email.startswith(name) and len(name) > 2:
						actual_name['contact_person']['name'].append(name)
		actual_name['contact_person']['name'] = list(set([x.encode('utf-8').strip() for x in actual_name['contact_person']['name']]))
		print actual_name
		#db.companies_data.update_one({'id': actual_name['id']}, {"$set": {'contact_person.name' : actual_name['contact_person']['name']}})

def find_cto_ceo(company_heads_list,company_heads_arg):
	count=0
	collection1=db1.distinct_data
	documents=collection1.find({'status' : '1'},{'_id':0})
	stop_words = set(stopwords.words('english'))
	for document in documents:
		business_heads_dict = {"name": "","email": "","number": "","linkedin": ""}
		names = {company_heads_arg : []}
		temp = {}
		data = {company_heads_arg : []}
		data['id'] = document['id'].encode('utf-8').strip()
		count+=1
		print count
		print document['url']
		for business in company_heads_list:
			for section in document['section']:
				temp['data'] = ' '.join(section['data'].lower().encode('utf-8','strict').replace('\t',' ').replace('\\t',' ').replace('\r',' ').replace('\\r',' ').replace('\s+',' ').replace('!',' ').replace('/',' ').replace('\n',' ').replace('\\n',' ').strip().split()) 
				if business in temp['data']:
					names[company_heads_arg].append(re.findall('([\w]+[\s]+[\w]+[\s][\w]+[\s][\w]+[\s]{business})'.format(business=business),str(temp['data'])))
		names[company_heads_arg] = list(set(chain(*names[company_heads_arg])))
		#print names[company_heads_arg]
		document =collection.find({},{'_id':0})
		for doc in document:
			if data['id'] == doc['id']:
				for name in doc['name']:
					business_heads_dict = {"name": "","email": "","number": "","linkedin": ""} 
					for heads in names[company_heads_arg]:
						if name in heads and len(name) >2:
							business_heads_dict['name'] = name
							data[company_heads_arg].append(business_heads_dict)
				print data
				#db.name_db.update_one({'id': data['id']}, {"$set": data})

technology_head = 'technology_heads'

def map_cto_ceo_name_email(company_heads_arg):
	documents=collection.find({},{'_id':0})
	count=0
	for document in documents:
		count+=1
		name_list = []
		data = {}
		actual_company_name = {}
		print count
		data['id'] = document['id']
		actual_company_name['name'] = document['url'].encode('utf-8').strip() 
		#print actual_company_name['name']
		if not document['url'].encode('utf-8').strip().endswith('/'):
			actual_company_name['name'] = document['url'].encode('utf-8').strip() + "/"
		actual_company_name['name'] = re.findall(r'//[a-zA-Z0-9\W]+/',str(actual_company_name['name']))
		temp = ''.join(str(x).encode('utf-8','strict').replace('/','') for x in actual_company_name['name'])
		if temp.startswith('www'):
			temp = temp.replace('www.','')    
		actual_company_name['name'] = temp.split('.')
		actual_company_name['name'] = actual_company_name['name'][0]
		if len(document[company_heads_arg]) > 0:
			names = [x['name'] for x in document[company_heads_arg]]
			for name in names:
				name_list.append(name)
		name_list = [x for x in list(set(name_list)) if actual_company_name['name'] not in x ]
		data[company_heads_arg] = []
		business_heads_dict = {"name": "","email": "","number": "","linkedin": ""}
		for distinct_name in name_list:
			#print distinct_name
			for email in document['email']:
				if email.startswith(distinct_name):
					print distinct_name
					print email

def match_email_and_cto_ceo():
	count = 0
	for document in documents:
		count+=1
		business_heads_dict = {"name": "","email": "","number": "","linkedin": ""}
		data = {'business_heads_with_email' : []}
		data['id'] = document['id']
		print count
		print document['url']
		for business in document['business_heads']:
			for email in list(set(document['email'])):
				if email.startswith(business['name']):
					print email
					print business['name']
