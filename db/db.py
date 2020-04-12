import os
import sys
import re

#def print_prompt():
EXIT_SUCCESS = 0
STATEMENT_INSERT = 0
STATEMENT_SELECT = 1

EXECUTE_SUCCESS = "EXECUTE_SUCCESS"
EXECUTE_TABLE_FULL = "EXECUTE_TABLE_FULL"

PREPARE_SUCCESS = "PREPARE_SUCCESS"
PREPARE_SYNTAX_ERROR = "PREPARE_SYNTAX_ERROR"
PREPARE_UNRECOGNIZED_STATEMENT = "PREPARE_UNRECOGNIZED_STATEMENT"

ROW_SIZE = 291
PAGE_SIZE = 4096
TABLE_MAX_PAGES = 100
ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE
TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES


def new_table():
    table = {
        "num_rows": 0,
        "pages": [] #[None] * TABLE_MAX_PAGES
    }

    return table


def row_slot(table, row_num):    
    page_num = row_num / ROWS_PER_PAGE
    pages = table.get("pages")

    #print(f">row_slot - page_num: {page_num} pages: {len(pages)}")

    if page_num >= len(pages):
        #Allocate memory only when we try to access page        
        pages.append([])

    page_num_int = int(page_num)
    return page_num_int, int(row_num - page_num_int * PAGE_SIZE)


def print_row(row):
    print(f">print_row {row}") 
    print("({}, {}, {})".format(row[0], row[1], row[2]))


def is_meta(cmd):
    ret = False

    if cmd.startswith("."):
        ret = True

    return ret


def prepare_row(id, username, email):
    return {
        "id": id,
        "username": username,
        "email": email
    }


def process_meta_command(cmd):
    ret = {
       "success": False,
       "error": ""
    }

    if cmd == ".exit":
        print("That's all folks!")
        sys.exit()
    else:
        ret["success"] = False
        ret["error"] = "META_COMMAND_UNRECOGNIZED_COMMAND"
    
    return ret


def prepare_statement(cmd):
    ret = {
        "success": False,
        "statement": {
            "type": "",
            "row_to_insert": ""
        },
        "error": PREPARE_UNRECOGNIZED_STATEMENT
    }

    if cmd.lower().startswith("insert"):
        match = re.match(r'insert (\d+) (.+) (.+)', cmd, re.IGNORECASE)
        if match:
            ret["success"] = True
            ret["error"] = ""
            ret["statement"]["type"] = STATEMENT_INSERT
            print(match.group(1), match.group(2), match.group(3))
            ret["statement"]["row_to_insert"] = [match.group(1), match.group(2), match.group(3)]
        else:
            ret["error"] = PREPARE_SYNTAX_ERROR
    elif cmd.lower().startswith("select"):
        ret["success"] = True
        ret["statement"]["type"] = STATEMENT_SELECT

    return ret


def execute_insert(statement, table):
    if table["num_rows"] >= TABLE_MAX_ROWS:
        return EXECUTE_TABLE_FULL
    
    #Row* row_to_insert = &(statement->row_to_insert)
    page_num, index = row_slot(table, table["num_rows"])
    
    page = table["pages"][page_num]
    page.append(statement["statement"]["row_to_insert"])
    
    table["num_rows"] += 1

    return EXECUTE_SUCCESS


def execute_select(statement, table):
    row = ""
    
    for i in range(0, table.get("num_rows")):
        page_num, index = row_slot(table, i)
        #print(f"page_num: {page_num}, index: {index}, table: {table}")
        print_row(table.get("pages")[page_num][index])

    return EXECUTE_SUCCESS


def execute_statement(statement, table):
    print(f"Executing statement '{statement}'.")
    
    ret = {
        "success": False,
        "error": "Unk"
    }

    if statement["statement"]["type"] == STATEMENT_INSERT:
        ret = execute_insert(statement, table)
    elif statement["statement"]["type"] == STATEMENT_SELECT:
        ret = execute_select(statement, table)

    return ret


if __name__ == "__main__":
    cmd = ""
    table = new_table()

    while True:
        cmd = input("db > ")
        
        if is_meta(cmd):
            ret = process_meta_command(cmd)
            if ret["success"]:
                pass
            else:
                print(f"Unrecognized command '{cmd}'.")
        else:
            statement = prepare_statement(cmd)
            if statement.get("success"):
                exec_sttm_res = execute_statement(statement, table)
                if exec_sttm_res == EXECUTE_SUCCESS:
                    print("Executed.")
                elif exec_sttm_res == EXECUTE_TABLE_FULL:
                    print("Error: Table full.")
                else:
                    print("Error: Unknown error.")
            elif statement.get("error") == PREPARE_SYNTAX_ERROR:
                print("Syntax error. Could not parse statement.")
            elif statement.get("error") == PREPARE_UNRECOGNIZED_STATEMENT:
                print(f"Unrecognized keyword at start of '{cmd}'.")
