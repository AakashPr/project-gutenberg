################################################################# IMPORTS #################################################################

import requests
import json
import simplejson
import traceback
import datetime
import time
import ssl
from flask import Flask,jsonify,request
import logging
import os
import warnings
import psycopg2

################################################################# GLOBALS ###############################################################

app = Flask(__name__)
conn = psycopg2.connect(database="postgres", user='aakash',password="aakash@123",host='127.0.0.1', port= '5432')
cursor = conn.cursor()
warnings.simplefilter("ignore")

if os.path.isdir("./logs") == False:
	os.system("mkdir ./logs")

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s")
file_handler = logging.FileHandler('./logs/gutenberg.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

################################################################# CERTIFICATES ###############################################################

# context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
# context.load_cert_chain("cert.pem", "key.pem")

################################################################# FUNCTIONS ###############################################################

def get_bookid_by_gutenberg_id(gutenberg_ids_string):
	try:
		books_gutenberg_id_query = f"SELECT gutenberg_id FROM books_book WHERE gutenberg_id IN ({gutenberg_ids_string});"
		try:
			cursor.execute(books_gutenberg_id_query)
		except:
			conn.rollback()
			logger.exception("")
		else:
			book_id_list = cursor.fetchall()
			return book_id_list
	except:
		logger.exception("")
		return []


def get_bookid_by_language(languages_string):
	try:
		books_language_query = f"SELECT book_id FROM books_book_languages WHERE language_id IN (SELECT id FROM books_language WHERE code IN ({languages_string}));"
		try:
			cursor.execute(books_language_query)
		except:
			conn.rollback()
			logger.exception("")
		else:
			book_id_list = cursor.fetchall()
			return book_id_list
	except:
		logger.exception("")
		return []


def get_bookid_by_mime_type(mime_type_string):
	try:
		# Fetches Book IDs of given mime types
		books_mime_type_query = f"SELECT book_id FROM books_format WHERE mime_type IN ({mime_type_string});"
		try:
			cursor.execute(books_mime_type_query)
		except:
			conn.rollback()
			logger.exception("")
		else:
			book_id_list = cursor.fetchall()
			return book_id_list
	except:
		logger.exception("")
		return []


def get_bookid_by_topics(topics_string):
	try:
		book_id_set = set()
		topics_list = topics_string.split("|")
		for each_topic in topics_list:
			try:
				books_subject_query = f"SELECT book_id FROM books_book_subjects WHERE subject_id IN (SELECT id FROM books_subject WHERE name ILIKE '%{each_topic}%');"
				books_bookshelf_query = f"SELECT book_id FROM books_book_bookshelves WHERE bookshelf_id IN (SELECT id FROM books_bookshelf WHERE name ILIKE '%{each_topic}%');"
				try:
					cursor.execute(books_subject_query)
				except:
					conn.rollback()
					logger.exception("")
				else:
					book_id_set = book_id_set.union(set(cursor.fetchall()))

				try:
					cursor.execute(books_bookshelf_query)
				except:
					conn.rollback()
					logger.exception("")
				else:
					book_id_set = book_id_set.union(set(cursor.fetchall()))
			except:
				logger.exception("")

		book_id_list = list(book_id_set)
		return book_id_list
	except:
		logger.exception("")
		return []


def get_bookid_by_author(authors_name_string):
	try:
		book_id_set = set()
		authors_list = authors_name_string.split("|")

		for each_author in authors_list:
			try:
				books_author_query = f"SELECT book_id FROM books_book_authors WHERE author_id IN (SELECT id FROM books_author WHERE name ILIKE '%{each_author}%');"
				try:
					cursor.execute(books_author_query)
				except:
					conn.rollback()
					logger.exception("")
				else:
					book_id_set = book_id_set.union(set(cursor.fetchall()))
			except:
				logger.exception("")

		book_id_list = list(book_id_set)
		return book_id_list
	except:
		logger.exception("")
		return []


def get_bookid_by_title(title_string):
	try:
		book_id_set = set()
		titles_list = title_string.split("|")

		for each_title in titles_list:
			try:
				books_title_query = f"SELECT gutenberg_id FROM books_book WHERE title ILIKE '%{each_title}%';"
				try:
					cursor.execute(books_title_query)
				except:
					conn.rollback()
					logger.exception("")
				else:
					book_id_set = book_id_set.union(set(cursor.fetchall()))
			except:
				logger.exception("")

		book_id_list = list(book_id_set)
		return book_id_list
	except:
		logger.exception("")
		return []


################################################################################################################################

def get_book_detail_by_book_id(book_ids_string,start_row,end_row):
	try:
		books_response_list = []
		ordered_book_list = []

		# query to fetch books
		books_book_query = f"SELECT gutenberg_id,title FROM books_book WHERE gutenberg_id IN ({book_ids_string}) AND download_count IS NOT NULL ORDER BY download_count DESC OFFSET {start_row} LIMIT {end_row};"
		try:
			cursor.execute(books_book_query)
		except:
			logger.exception("")
			return books_response_list
		else:
			for each_book in cursor.fetchall():
				#remove download count later
				books_response_list.append({"gutenberg_id":each_book[0],
											"title":each_book[1],
											# "download_count":each_book[2],
											"author":"",
											"language":"",
											"subject":[],
											"bookshelf":[],
											"urls":[],
											})
				ordered_book_list.append(each_book[0])

			ordered_book_string = str(ordered_book_list).replace("[","").replace("]","")

			books_author_query = f"SELECT book_id,name FROM books_author,books_book_authors WHERE books_author.id = books_book_authors.author_id AND books_book_authors.book_id IN ({ordered_book_string});"
			# books_genre_query = ""
			books_language_query = f"SELECT book_id,code FROM books_language,books_book_languages WHERE books_language.id = books_book_languages.language_id AND books_book_languages.book_id IN ({ordered_book_string});"
			books_subject_query = f"SELECT book_id,name FROM books_book_subjects,books_subject WHERE books_book_subjects.subject_id = books_subject.id AND books_book_subjects.book_id IN ({ordered_book_string});"
			books_bookshelf_query = f"SELECT book_id,name FROM books_book_bookshelves,books_bookshelf WHERE books_book_bookshelves.bookshelf_id = books_bookshelf.id AND books_book_bookshelves.book_id IN ({ordered_book_string});"
			books_mime_type_query = f"SELECT book_id,url FROM books_format WHERE book_id IN ({ordered_book_string})"


			try:
				cursor.execute(books_author_query)
			except:
				logger.exception("")
			else:
				author_data = cursor.fetchall()

			try:
				cursor.execute(books_language_query)
			except:
				logger.exception("")
			else:
				language_data = cursor.fetchall()

			try:
				cursor.execute(books_subject_query)
			except:
				logger.exception("")
			else:
				subject_data = cursor.fetchall()

			try:
				cursor.execute(books_bookshelf_query)
			except:
				logger.exception("")
			else:
				bookshelf_data = cursor.fetchall()

			try:
				cursor.execute(books_mime_type_query)
			except:
				logger.exception("")
			else:
				mime_type_data = cursor.fetchall()

			# Iterates through the sorted matched books list and fills in author,language,bookshelf,subject,mime_type detail by my matching gutenberg_id with data fetched from respective tables
			for each_book in books_response_list:
				try:
					for each_author in author_data:
						try:
							if each_book["gutenberg_id"] == each_author[0]:
								each_book["author"] = each_author[1]
								break
						except:
							logger.exception("")

					for each_language in language_data:
						try:
							if each_book["gutenberg_id"] == each_language[0]:
								each_book["language"] = each_language[1]
								break
						except:
							logger.exception("")

					each_book_subjects = []
					for each_subject in subject_data:
						try:
							if each_book["gutenberg_id"] == each_subject[0]:
								each_book_subjects.append(each_subject[1])
						except:
							logger.exception("")
					each_book["subject"] = each_book_subjects

					each_book_bookshelves = []
					for each_bookshelf in bookshelf_data:
						try:
							if each_book["gutenberg_id"] == each_bookshelf[0]:
								each_book_bookshelves.append(each_bookshelf[1])
						except:
							logger.exception("")
					each_book["bookshelf"] = each_book_bookshelves

					each_book_urls = []
					for each_mime_type in mime_type_data:
						try:
							if each_book["gutenberg_id"] == each_mime_type[0]:
								each_book_urls.append(each_mime_type[1])
						except:
							logger.exception("")
					each_book["urls"] = each_book_urls

					del each_book["gutenberg_id"]
				except:
					logger.exception("")

			return books_response_list
	except:
		logger.exception("")
		return []


################################################################# API Calls ##################################################################

@app.route('/get_books', methods=['GET'])
def get_books():
	try:
		filter_parameters = request.args.to_dict()
		matched_book_ids = set()

		# gutenberg_id needs to be string => 1,2,3 or 1
		if "gutenberg_ids" in filter_parameters:
			try:
				guten_book_list = get_bookid_by_gutenberg_id(filter_parameters["gutenberg_ids"])
			except:
				pass
			else:
				matched_book_ids = matched_book_ids.union(set(guten_book_list))

		# languages needs to be like 'en','fr','es' or 'en'
		if "languages" in filter_parameters:
			try:
				lang_book_list = get_bookid_by_language(filter_parameters["languages"])
			except:
				pass
			else:
				if not matched_book_ids:
					matched_book_ids = matched_book_ids.union(set(lang_book_list))
				else:
					matched_book_ids = matched_book_ids.intersection(set(lang_book_list))

		# mime-types needs to be like 'text/plain' or 'text/plain','text/html'
		if "mime_types" in filter_parameters:
			try:
				mime_book_list = get_bookid_by_mime_type(filter_parameters["mime_types"])
			except:
				pass
			else:
				if not matched_book_ids:
					matched_book_ids = matched_book_ids.union(set(mime_book_list))
				else:
					matched_book_ids = matched_book_ids.intersection(set(mime_book_list))

		# topics needs to be like child or child|politics|horror
		if "topics" in filter_parameters:
			try:
				topics_book_list = get_bookid_by_topics(filter_parameters["topics"])
			except:
				pass
			else:
				if not matched_book_ids:
					matched_book_ids = matched_book_ids.union(set(topics_book_list))
				else:
					matched_book_ids = matched_book_ids.intersection(set(topics_book_list))

		# author needs to be like jefferson or jefferson|Aesop|dickens
		if "authors" in filter_parameters:
			try:
				author_book_list = get_bookid_by_author(filter_parameters["authors"])
			except:
				pass
			else:
				if not matched_book_ids:
					matched_book_ids = matched_book_ids.union(set(author_book_list))
				else:
					matched_book_ids = matched_book_ids.intersection(set(author_book_list))

		# title needs to be like Mayflower or Mayflower|Peter pan|Alice
		if "titles" in filter_parameters:
			try:
				title_book_list = get_bookid_by_title(filter_parameters["titles"])
			except:
				pass
			else:
				if not matched_book_ids:
					matched_book_ids = matched_book_ids.union(set(title_book_list))
				else:
					matched_book_ids = matched_book_ids.intersection(set(title_book_list))

		# checks whether page parameter has been inputted in the url parameter, if not then defaults it to 1
		if "page" in filter_parameters:
			page_no = abs(int(filter_parameters["page"]))
		else:
			page_no = 1

		total_books = len(matched_book_ids)
		no_of_pages = total_books/25

		# Sets the start and end rows to be return from the total books which matched the filters
		if page_no <= no_of_pages and page_no != 0:
			start_row = (page_no - 1) * 25
			end_row = page_no * 25
		else:
			start_row = 0
			end_row = 25

		# format inputted list from [(1,),(2,),(3,)] to [1,2,3]
		final_book_id_list = []
		for each_book_id in matched_book_ids:
			final_book_id_list.append(each_book_id[0])

		# convert the list to string and format it from [1,2,3]  to 1,2,3, so that it can be dirctly used in the sql query
		book_ids_string = str(final_book_id_list).replace("[","").replace("]","")

		# Makes call to the function which takes books ids, start row, end row and responds with list of dictionaries with complete information about each book inputted
		complete_books_info_list = []
		if matched_book_ids != set():
			complete_books_info_list = get_book_detail_by_book_id(book_ids_string,start_row,end_row)

		return jsonify({"status":"success","book_list":complete_books_info_list,"number_of_book_matched":len(final_book_id_list)}),200
	except:
		logger.exception("")
		return jsonify({"status":"failure","book_list":[],"number_of_book_matched":0}),500

################################################################# THREADS ##################################################################






################################################################# FLASK APP RUN ############################################################

# if __name__ == "__main__":
# 	app.run(host="0.0.0.0",port=5000,debug=True,ssl_context=context)
