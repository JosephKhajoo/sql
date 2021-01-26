import sqlite3
import logging
from sqlite3 import Error
import json
import os

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(asctime)s - %(message)s")


def select(database, query):
	try:
		conn = sqlite3.connect(database)
		logging.info(f"Created database {database} inside of `select` function.")

	except Error as e:
		print(e)

	curs = conn.cursor()


	curs.execute(query)

	result_list = curs.fetchall()
	logging.debug(f"Got the result list from `{query}` {len(result_list)}, {result_list}")
	for result_tup in result_list:
		for result in result_tup:
			print(result)


def dump_into_json(database, select_query):
	try:
		conn = sqlite3.connect(database)
		logging.info(f"Created database {database} inside of `dump_into_json` function.")
		curs = conn.cursor()
	except Error as e:
		print(e)

	curs.execute(select_query)

	result_list = curs.fetchall()
	with open("file_dumped.json", "w") as file:
		json.dump(result_list, file, indent=2)


db = os.path.join(os.getcwd(), "film.db")

query = """SELECT title FROM film WHERE title LIKE 'B%';"""
largest_query = """ SELECT title, max(length) AS length FROM film; """
json_query = """SELECT * FROM film;"""

# select(db, query)
# select(db, largest_query)
# dump_into_json(db, json_query)


##################################################################

# extra** write script which finds all the movies from film table
# which release date is above 2010 and rate is between 3 and 5,
# after that writes them in a new table called filtered_film

##################################################################


extra_query = """ SELECT * FROM film
WHERE release_year > 2010
AND rate 
BETWEEN 3 AND 5"""


def filter_table(database, query):
	try:
		conn = sqlite3.connect(database)
		curs = conn.cursor()
		logging.info(f"Created database {database} inside of `filter_table` function.")
	except Error as e:
		print(e)

	new_table = "filtered_film"

	curs.execute(query)

	result_list = curs.fetchall()
	curs.execute("""CREATE TABLE {} AS SELECT * FROM film WHERE 0;""".format(new_table))
	logging.info(f"Table `{new_table}` created succesfully.")
	
	table_create = """INSERT INTO {}(film_id, title, description, release_year, rate, length, special_features)
VALUES (?, ?, ?, ?, ?, ?, ?)""".format(new_table)
	for i in result_list:
		curs.execute(table_create, i)
	conn.commit()


# filter_table(db, extra_query)
