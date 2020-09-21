import pandas
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

conn = snowflake.connector.connect(
          user='DARSHI',
          account='ni10492.us-central1.gcp',
          password='Chathu@1992',
          warehouse='COMPUTE_WH',
          database='TEST_DATA',
          role='SYSADMIN',
          schema='PUBLIC')

print('connect')
df1= pandas.read_json(r'C:\Users\chathu\Desktop\wiley\course_data.json')

cur = conn.cursor()
cur.execute("""TRUNCATE TABLE TEST_DATA.PUBLIC.COURSE_SALE """)

df1 = df1[['course_id',
          'course_title',
           'is_paid',
           'num_subscribers',
           'price',
           'author',
           'content_duration',
           'level',
           'num_lectures',
           'num_reviews',
           'published_timestamp',
           'subject']]

df1.rename(columns={'course_id':'COURSE_ID',
                    'course_title':'COURSE_TITLE',
                    'is_paid':'IS_PAID',
                    },
           inplace=True)
print(df1)
try:
    write_pandas(conn, df1, 'COURSE_SALE')
    conn.commit()
finally:
    cur.close()





