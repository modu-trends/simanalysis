import pymysql
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
import sys
from konlpy.tag import Kkma
from konlpy.utils import pprint
from konlpy.tag import Twitter
'''
CREATE TABLE simanalysis( id char(10) NOT NULL, token text(65535), sim1_id char(10), sim2_id char(10), sim3_id char(10), tokenizing_status int, PRIMARY KEY(id));
tokenizing_status 0 : progress and *
tokenizing_status 1 : expired and non-complete tokenize
tokenizing_status 2 : expired and complete tokenize
'''

def update_token(petition_id, token_content,tokenizing_status): 
	print("update")
	sys.exit()
	#try:
	sql = """INSERT INTO simanalysis (id, token, tokenizing_status) VALUES (%s, %s, %s)"""

	print(petition_id,token_content,tokenizing_status)
	token_content_str=' '.join(token_content).strip()
	print(token_content_str)
	curs.execute(sql, (petition_id, token_content, tokenizing_status))
	conn.commit()

	#except:
	#    print("# insert except!!")
	#    sys.exit() 


def insert_token(petition_id, petition_content, tokenizing_status): 
	sql = "SELECT status FROM petition_crawled WHERE id = %s" # get crawled status
	curs.execute(sql, petition_id)
	crawled_status = curs.fetchone()

	if crawled_status != None: # if item already exists
		tokenizing_status = crawled_status[0] # progress -> progress
	else :
		print("Error!! non crawled_status of petition_crawled")
		sys.exit()

	twitter = Twitter()#for tokenizing made by twitter
	token_content = twitter.morphs(petition_content)


	#try:
	sql = """INSERT INTO simanalysis (id, token, tokenizing_status) VALUES (%s, %s, %s)"""

	token_content_str=' '.join(token_content).strip()
	print(token_content_str,tokenizing_status)
	#curs.execute(sql, (petition_id, token_content_str, tokenizing_status))
	#conn.commit()

	#except:
	#    print("# insert except!!")
	#    sys.exit() 


def tokenize_petition(petition_id,petition_content):
	sql = "SELECT tokenizing_status FROM simanalysis WHERE id = %s"
	curs.execute(sql, petition_id)
	tokenizing_status = curs.fetchone()

	if tokenizing_status != None :
		if tokenizing_status[0]==0:
			update_token(petition_id, token_content,tokenizing_status)
		elif tokenizing_status[1]==0:
			return 1 #completed tokenize ,so nomore tokenizing
	else :
		insert_token(petition_id, petition_content, tokenizing_status)

def get_crawled_db():
	#sql = "SELECT id, content FROM petition_crawled"
	sql = "SELECT id, content FROM petition_crawled limit 100"
	curs.execute(sql)
	
	content_cnt = curs.rowcount
	print("rowcount : ", curs.rowcount)
	rows = curs.fetchall() # get all DB

	for row in rows:
		petition_id = row[0]
		petition_content = row[1]
		tokenize_petition(petition_id,petition_content)

	return 0


def petition_content2vec():
	print("petition_content2vec")
	word_list = list(get_db())

	documents = [TaggedDocument(doc, [i]) for i, doc in enumerate(word_list)]

	model = Doc2Vec(documents, vector_size=200, window=50, min_count=1, workers=1, epochs=30)
	model.save(fname)


	#Convert the sample document into a list and use the infer_vector method to get a vector representation for it
	new_doc_words = test_sample.split()
	new_doc_vec = model.infer_vector(new_doc_words, steps=50, alpha=0.25)

	#use the most_similar utility to find the most similar documents.
	similars = model.docvecs.most_similar(positive=[new_doc_vec])


if __name__ == "__main__":
	conn = pymysql.connect(host='35.200.82.12', port=3306, user='modutrend', password='trend1q2w3e!@',db='petition', charset='utf8mb4', )
	curs = conn.cursor()

	print("connect success")
	get_crawled_db()
	print("end get_crawled_db")


	#petition_content2vec()
