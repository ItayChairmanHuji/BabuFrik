import logging
import time


def create_db_table_from_query(self, name, query):
    tic = time.clock()
    drop = drop_table_template.substitute(table=name)
    create = create_table_template.substitute(table=name, stmt=query)
    with self.engine.begin() as conn:
        conn.execute(drop)
        conn.execute(create)
    toc = time.clock()
    logging.debug('Time to create table: %.2f secs', toc - tic)
    return True

def import_fixes():
    holoclean.dataset.dbengine.DBengine.create_db_table_from_query = create_db_table_from_query
