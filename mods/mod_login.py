import psycopg2
import psycopg2.extras
import psycopg2.extensions
from qgis.PyQt.QtWidgets import QMessageBox


class Database:
    def __init__(self, parent=None, main=None, dic_conn={}):
        self.parent = parent
        self.main = main
        self.dic_conn = dic_conn
        self.cur = False
        self.conn = False
        self.conn_name = ''
        self.db_name = ''
        self.connect_()

    def objectName(self):
        return 'Database'

    # criar conexão
    def connect_(self, con_name=''):
        try:
            self.conn_name = self.dic_conn['name']['value']
            self.conn = psycopg2.connect(
                database=self.dic_conn['db']['value'],
                user=self.dic_conn['user']['value'],
                password=self.dic_conn['pass']['value'],
                host=self.dic_conn['host']['value'],
                port=self.dic_conn['port']['value'])
            self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            self.db_name = self.dic_conn['db']['value']
        except Exception as e_:
            self.conn_name = ''
            # QMessageBox.critical(None, 'Problema conexão', str(e_))
            self.cur = False
            self.conn = False
            return False
        return True

    def select_(self, query):
        try:
            self.cur.execute(query)
            return self.cur.fetchall()
        except Exception as e:
            return [e]

    def query_(self, query):
        self.cur.execute(query)
        # self.conn.commit()

    def commit_(self):
        self.conn.commit()

    def rollback_(self):
        self.conn.rollback()

    def close_(self):
        print('close_')
        if self.conn:
            self.cur.close()
            self.conn.close()
            self.con_name = ''

    def is_connected(self):
        status_ = psycopg2.extensions.ConnectionInfo(self.conn).transaction_status
        # print('is_connected-status=', status_ == 0)
        if status_ == 0:
            return True
        else:
            return False

