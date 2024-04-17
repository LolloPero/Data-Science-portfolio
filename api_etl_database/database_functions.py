import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
import typing

from configparser import ConfigParser
from psycopg2 import sql
from psycopg2.extras import execute_values

## Constants
_BAD_WORDS = set([";","drop","rm","-r","delete"])


######################### GENERAL DATABASE MANAGEMENT ##########################

def get_config(filename:str, section:str):
    """ 
        utility function to read a configuration file as a dictionary
    """

    parser = ConfigParser()

    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


class cryptodb_connection(object):
    """ Database connection context manager.
    
    Context manager to execute custom queries.

    Usage: 
    with cryptodb_connection() as db:

    Optional usage: 
    with cryptodb_connection() as db:
        with db.conn.cursor as curs:
    
    You can use db.sql to compose SQL strings.
    See: https://www.psycopg.org/docs/sql.html

    You can use db.execute_values for performant batch inserts.
    See: https://www.psycopg.org/docs/extras.html?highlight=execute_values
    #psycopg2.extras.execute_values
    """
    def __init__(self):
        self.conn = None 
        self.sql = sql ## SQL construction tool
        self.execute_values = execute_values
        
    def __enter__(self):
        self.config = get_config(   filename="database.ini",
                                    section="crypto")
        self.conn = psycopg2.connect(**self.config)
        return self
        
    def __exit__(self, type, value, traceback):
        if traceback is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        self.conn.close()


def print_dbv():
    with cryptodb_connection() as db:
            with db.conn.cursor() as curs:
                curs.execute('SELECT version()')
                db_version = curs.fetchone()
                print(db_version)
    return


def insert_values(table:str,input_dict:dict):
    """
    - table:     name of the table to be inserted
    - input_dict:
                col_1 : list_1
                col_2 : list_2
                ...     
    """
    #replace NAN values with None, to allow SQL to insert NULL values
    for key,value in input_dict.items():
        new_value = [x if str(x) != "nan" else None for x in value]
        input_dict[key] = new_value

    query = sql.SQL(""" INSERT INTO {}({}) VALUES({}) 
                        ON CONFLICT DO NOTHING;""").format(
                        sql.Identifier(table),
                        sql.SQL(', ').join(map(sql.Identifier, input_dict.keys())),
                        sql.SQL(', ').join(sql.Placeholder() * len(input_dict.keys())))
    
    #assemble values to insert
    values=list(zip(*[l for l in input_dict.values()]))

    with cryptodb_connection() as db:
            with db.conn.cursor() as curs:                              
                curs.executemany(query,values)


def general_query(  qry: typing.Union[sql.Composed,str], 
                    nr_to_return: int=None, 
                    filter_by: typing.Union[sql.Composed,str] = '', 
                    order_by: str = '',
                    db: cryptodb_connection=None ) -> pd.core.frame.DataFrame:
    
    """ 
        Executes a general query 
    """
    def __query( qry, nr_to_return, filter_by, order_by, db ):

        with db.conn.cursor() as curs:
            if type(qry) == str:
                base_qry = qry
            elif type(qry) == sql.Composed:
                base_qry = qry.as_string(curs)

            if type(filter_by) == sql.Composed:
                filter_by = filter_by.as_string(curs)    
            filter_by = filter_by.replace('%','%%')

            if len(filter_by) > 0:
                base_qry += ' %s ' % (filter_by)

            # Order by
            if len(order_by) > 0:
                base_qry += ' ORDER BY %s ' % (order_by)
            # Limit    
            if nr_to_return:
                base_qry += sql.SQL(' LIMIT {} ').format( 
                    sql.Literal(nr_to_return) ).as_string(curs)
            
            #print(f"\nquery: {base_qry}\n")
            return pd.read_sql_query(base_qry, db.conn)

    # if any(word in order_by or word in filter_by for word in _BAD_WORDS):
    #     raise(ValueError)

    if db is not None:
        return __query(qry, nr_to_return, filter_by, order_by, db)
    else:
        with cryptodb_connection() as db:
            return __query(qry, nr_to_return, filter_by, order_by, db)