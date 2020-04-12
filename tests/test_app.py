from db.db import new_table, is_meta, execute_insert, execute_select, EXECUTE_SUCCESS, EXECUTE_TABLE_FULL, prepare_statement, STATEMENT_INSERT, PREPARE_NEGATIVE_ID, PREPARE_STRING_TOO_LONG
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


def test_prepare_statement_insert_valid():
    sttm = "insert 1 user1 person1@example.com"
    ret = prepare_statement(sttm)
    
    assert ret.get("success")
    assert ret.get("statement").get("type") == STATEMENT_INSERT
    

def test_prepare_statement_insert_negative_id():
    sttm = "insert -1 cstack foo@bar.com"
    ret = prepare_statement(sttm)
    
    assert ret.get("success") == False
    assert ret.get("statement").get("type") == STATEMENT_INSERT
    assert ret.get("error") == PREPARE_NEGATIVE_ID


def test_prepare_statement_insert_username_too_long():
    sttm = "insert 1 {} {}".format("a" * 33, "foo@bar.com")
    ret = prepare_statement(sttm)
    
    assert ret.get("success") == False
    assert ret.get("statement").get("type") == STATEMENT_INSERT
    assert ret.get("error") == PREPARE_STRING_TOO_LONG
    

def test_prepare_statement_insert_email_too_long():
    sttm = "insert 1 {} {}".format("cstack", "f" * 256)
    ret = prepare_statement(sttm)
    
    assert ret.get("success") == False
    assert ret.get("statement").get("type") == STATEMENT_INSERT
    assert ret.get("error") == PREPARE_STRING_TOO_LONG
    

def test_execute_insert_valid():
    table = new_table()
    statement = { 
        "success": True,
        "statement": {
            "type": "insert",
            "row_to_insert": [1, 2, 3]
        }
    }

    ret = execute_insert(statement, table)

    assert ret == EXECUTE_SUCCESS
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