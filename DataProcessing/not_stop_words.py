from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
import demoji
import unicodedata as uni
import re
import mysql.connector
import config
import pandas as pd



def init_db():
    return mysql.connector.connect(
            host = config.configSQLCrawler.HOST,
            user = config.configSQLCrawler.USER,
            passwd = config.configSQLCrawler.PASSWD,
            database = config.configSQLCrawler.DATABASE,
            port = config.configSQLCrawler.PORT,
            auth_plugin=config.configSQLCrawler.AUTH_PLUGIN
        )

def get_cursor(con):
    try:
        con.ping(reconnect=True, attempts=3, delay=5)
    except mysql.connector.Error as e:
        raise Exception('db error')
    return con.cursor(buffered=True)

def handle_emoji(text):
    emoji_less_text = demoji.replace(text, "")

    return emoji_less_text

def handle_unicode(text):
    text = uni.normalize('NFKD', text)

    return text

def remove_stopwords(text, stopwordsfile):
    stopwords_list = [word.strip() for word in open(stopwordsfile).readlines()]
    return ' '.join([word.strip() for word in text.split() if word.strip() not in stopwords_list])

def stemming(text):
    return stemmer.stem(text)

# define function to pre-process text column
def preprocess(text):
    text = handle_emoji(text)
    text = handle_unicode(text)
    text = text.replace(r'\n', ' ')
    text = text.lower()
    text = re.sub('[^a-z ]+', '', text)
    # text = remove_stopwords(text, ".\stopwords\stopword_list1.txt")
    text = stemming(text)
    # text_list = [word.strip() for word in text.split()]
    return text

sql = """
        SELECT id, text
        FROM   hotel_review_filtered 
        WHERE  id NOT IN (SELECT review_id FROM not_yet_stopwords)
        AND lang=%s
        AND is_processed=0
        LIMIT 50
"""
args = ('in',)

factory = StemmerFactory()
stemmer = factory.create_stemmer()

processed_data = 0
while 1:
    db = init_db()
    cursor = get_cursor(db)
    cursor.execute(sql, args)
    id_text_data = cursor.fetchall()
    id_list = [(row[0],) for row in id_text_data]
    # print(id_list)
    cursor.executemany('UPDATE hotel_review_filtered SET is_processed=1 WHERE id=%s', (id_list))
    db.commit()
    len_row = len(id_text_data)
    if len_row == 0:
        break

    id_text_df = pd.DataFrame(id_text_data, columns=['id','text'])

    id_text_df['text'] = id_text_df['text'].apply(preprocess)
    list_to_insert = id_text_df.values.tolist()
    cursor.executemany('INSERT IGNORE INTO not_yet_stopwords (review_id, text) values (%s, %s)', list_to_insert)
    db.commit()
    cursor.executemany('UPDATE hotel_review_filtered SET is_processed=0 WHERE id=%s', (id_list))
    db.commit()
    processed_data += len_row
    print(f'\rProcessed data: {processed_data:<6}', end='')