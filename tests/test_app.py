from db.db import new_table, is_meta, execute_insert, execute_select, EXECUTE_SUCCESS, EXECUTE_TABLE_FULL
import json
import logging
import pprint

pp = pprint.PrettyPrinter(indent=4)

LOGGER = logging.getLogger(__name__)

def test_is_meta():
    # pylint: disable=W0612,W0613
    r = is_meta(".bla")
    
    assert r

def test_is_not_meta():
    # pylint: disable=W0612,W0613
    r = is_meta("bla")
    
    assert not r

def test_execute_insert():
    table = new_table()
    statement = { 
        "success": True,
        "statement": {
            "type": "insert",
            "row_to_insert": [1, 2, 3]
        }
    }

    execute_insert(statement, table)

    assert table["num_rows"] == 1
    assert table["pages"]
    assert table["pages"][0][0][0] == 1
    

def test_execute_select():
    table = new_table()
    statement = { 
        "success": True,
        "statement": {
            "type": "insert",
            "row_to_insert": [1, 2, 3]
        }
    }

    execute_insert(statement, table)
    ret = execute_select(statement, table)

    assert table["num_rows"] == 1
    assert table["pages"]
    assert ret == EXECUTE_SUCCESS
    
def test_table_full():
    table = new_table()
    statement = { 
        "success": True,
        "statement": {
            "type": "insert",
            "row_to_insert": [1, 2, 3]
        }
    }

    table_full_found = False
    
    for i in range(0, 1500):
        if execute_insert(statement, table) == EXECUTE_TABLE_FULL:
            table_full_found = True
            print(i)
            break

    assert table_full_found