import pymysql
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
import sys
from konlpy.tag import Kkma
from konlpy.utils import pprint
from konlpy.tag import Twitter

import multiprocessing

'''
CREATE TABLE simanalysis( id char(10) NOT NULL, token text(65535), sim1_id char(10), sim2_id char(10), sim3_id char(10), tokenizing_status int, PRIMARY KEY(id));
tokenizing_status 0 : progress and *
tokenizing_status 1 : expired and non-complete tokenize
tokenizing_status 2 : expired and complete tokenize
'''

word2vec_save_fname="petition_word2vec"

def get_crawled_status(petition_id):
	sql = "SELECT status FROM petition_crawled WHERE id = \"%s\"" # get crawled status
	curs.execute(sql, petition_id)
	crawled_status = curs.fetchone()

	if crawled_status != None: # if item already exists
		tokenizing_status = crawled_status[0] # progress -> progress
		return tokenizing_status
	else :
		print("petition_id:",petition_id)
		print("Error!! non crawled_status of petition_crawled")
		sys.exit()

def update_tokenize(petition_id, petition_content, tokenizing_status): 
	tokenizing_status = get_crawled_status(petition_id)

	twitter = Twitter()#for tokenizing made by twitter
	token_content = twitter.morphs(petition_content)
	token_content_str=' '.join(token_content).strip()

	sql = "UPDATE simanalysis SET token = %s, tokenizing_status = %s where id = \"%s\""
	#UPDATE simanalysis SET token = "ddjdjdjdkak", tokenizing_status = 0 where id = 21
	
	try:
		curs.execute(sql, (token_content_str, tokenizing_status, petition_id))
		conn.commit()

	except:
	    print("# update except!!")
	    sys.exit() 


def insert_tokenize(petition_id, petition_content, tokenizing_status): 
	tokenizing_status = get_crawled_status(petition_id)

	twitter = Twitter()#for tokenizing made by twitter
	token_content = twitter.morphs(petition_content)
	token_content_str=' '.join(token_content).strip()

	sql = "INSERT INTO simanalysis (id, token, tokenizing_status) VALUES (%s, %s, %s)"
	
	#print(sql, (petition_id, token_content_str, tokenizing_status))
	try:
		curs.execute(sql, (petition_id, token_content_str, tokenizing_status))
		conn.commit()

	except:
	    print("# insert except!!")
	    sys.exit() 


def check_tokenizing_status(petition_id,petition_content):
	sql = "SELECT tokenizing_status FROM simanalysis WHERE id = \"%s\""
	curs.execute(sql, petition_id)
	tokenizing_status = curs.fetchone()


	if tokenizing_status != None :
		if tokenizing_status[0]==0:
			update_tokenize(petition_id, petition_content,tokenizing_status)
		elif tokenizing_status[0]==1:
			return 1 #completed tokenize ,so nomore tokenizing
	else :
		insert_tokenize(petition_id, petition_content, tokenizing_status)


def check_tokenizing_status_works(row):
	petition_id = row[0]

	if str(petition_id) == '170365':#becuase junk petition_id
		print("170365 return")
		return 0
	petition_content = row[1]

	try:
		sql = "SELECT tokenizing_status FROM simanalysis WHERE id = \"%s\""
		curs.execute(sql, petition_id)
		tokenizing_status = curs.fetchone()
	except:
	    print("# select except!!")
	    sys.exit() 

	#print(tokenizing_status,petition_id)

	if tokenizing_status != None :
		if tokenizing_status[0]==0:
			update_tokenize(petition_id, petition_content,tokenizing_status)
		elif tokenizing_status[0]==1:
			#print("ok",petition_id)
			return 1 #completed tokenize ,so nomore tokenizing
	else :
		insert_tokenize(petition_id, petition_content, tokenizing_status)

def tokenize_crawled_db():
	#sql = "SELECT id, content FROM petition_crawled"
	sql = "SELECT id, content FROM petition_crawled"
	curs.execute(sql)
	
	content_cnt = curs.rowcount
	print("rowcount : ", curs.rowcount)
	rows = curs.fetchall() # get all DB


	batch_size=1000
	start = 227000
	end = 140000

	'''
	for idx in range(int(len(rows)/batch_size)+1):
		start = end
		end = start + batch_size
		pool = multiprocessing.Pool(processes=1)
		pool.map(check_tokenizing_status_works, rows[start:end])
		pool.close()

		print(start)
	'''		
	for idx, row in enumerate(rows):
		if idx <=  start:
			continue

		check_tokenizing_status_works(row)

		if idx%1000 == 0:
			print(idx)

	return 0


def get_tokenize_db():
	#sql = "SELECT id, token FROM simanalysis limit 10"
	sql = "SELECT id, token FROM simanalysis ORDER BY id"
	curs.execute(sql) 

	content_cnt = curs.rowcount
	print("rowcount : ", curs.rowcount)

	rows = curs.fetchall() # get all DB
	for row in rows:
		petition_id = row[0]
		petition_content = row[1]
		petition_content=petition_content.split()#string to list
		#print(petition_id,petition_content)

		yield petition_content

	return 0
def get_index_tokenize_db():
	sql = "SELECT id FROM simanalysis ORDER BY id"
	curs.execute(sql) 

	rows = curs.fetchall() # get all DB

	return rows


def petition_content2vec():
	print("petition_content2vec")
	word_list = list(get_tokenize_db())

	index_db = get_index_tokenize_db()

	documents = [TaggedDocument(doc, [index_db[i][0]]) for i, doc in enumerate(word_list)]

	model = Doc2Vec(documents, vector_size=200, window=50, min_count=1, workers=4, epochs=30)
	model.save(word2vec_save_fname)

	
	#Convert the sample document into a list and use the infer_vector method to get a vector representation for it
	new_doc_words = "블라인드 채용 을 추진 하신 의미 를 새겨 볼 때".split()
	new_doc_vec = model.infer_vector(new_doc_words, steps=50, alpha=0.25)

	#use the most_similar utility to find the most similar documents.
	similars = model.docvecs.most_similar(positive=[new_doc_vec])
	print(similars[0][0])

	print(model.docvecs.most_similar(positive=[new_doc_vec]))
	print(documents[6])

	


if __name__ == "__main__":

	conn = pymysql.connect(host='35.200.82.12', port=3306, user='modutrend', password='trend1q2w3e!@',db='petition', charset='utf8mb4', )
	curs = conn.cursor()

	print("connect success")

	tokenize_crawled_db()

	print("end get_crawled_db")


	#petition_content2vec()
