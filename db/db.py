import os
import sys
import re

#def print_prompt():
EXIT_SUCCESS = 0
EXIT_FAILURE = 1

STATEMENT_INSERT = 0
STATEMENT_SELECT = 1

EXECUTE_SUCCESS = "EXECUTE_SUCCESS"
EXECUTE_TABLE_FULL = "EXECUTE_TABLE_FULL"

PREPARE_SUCCESS = "PREPARE_SUCCESS"
PREPARE_NEGATIVE_ID = "PREPARE_NEGATIVE_ID"
PREPARE_STRING_TOO_LONG = "PREPARE_STRING_TOO_LONG"
PREPARE_SYNTAX_ERROR = "PREPARE_SYNTAX_ERROR"
PREPARE_UNRECOGNIZED_STATEMENT = "PREPARE_UNRECOGNIZED_STATEMENT"

ROW_SIZE = 291
PAGE_SIZE = 4096
TABLE_MAX_PAGES = 100
ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE
TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES

COLUMN_USERNAME_SIZE = 32
COLUMN_EMAIL_SIZE = 255


def pager_open(filename):
    fd = None
    
    try:
        fd = os.open(filename, os.O_RDWR | os.O_CREAT)
    except os.error as e:
        print("Unable to open file.")
        sys.exit(EXIT_FAILURE)

    file_length = os.lseek(fd, 0, os.SEEK_END)

    pager = {
        "file_descriptor": fd,
        "file_length": file_length,
        "pages": []
    }
    
    for i in range(0, TABLE_MAX_PAGES):
        pager.get("pages").append(None)

    return pager


def db_open(filename):
    pager = pager_open(filename)
    num_rows = pager["file_length"] / ROW_SIZE
    table = {
        "num_rows": num_rows,
        "pager": pager
    }

    return table


#def pager_flush(pager, page_num, size):
def get_page_element_bytes(pe):
    b = bytes("{:4}{:32}{:255}".format(pe[0], pe[1], pe[2]), 'utf8')
    print(f"get_page_element_bytes: '{b}'")
    
    return b


def write_page(fd, page):
    print(f">write_page {fd}, {page}")
    
    for i in range(0, len(page)):
        os.write(fd, get_page_element_bytes(page[i]))

def pager_flush(pager, page_num, size):
    if pager.get("pages")[page_num] == None:
        print(f"Tried to flush null page")
        sys.exit(EXIT_FAILURE)

    try:
        os.lseek(pager.get("file_descriptor"), page_num * PAGE_SIZE, os.SEEK_SET)
    except os.error as e:
        print(f"Error seeking: {e.errno}")
        sys.exit(EXIT_FAILURE)

    try:
        write_page(pager.get("file_descriptor"), pager.get("pages")[page_num])
    except os.error as e:
        print(f"Error writing: {e.errno}")
        sys.exit(EXIT_FAILURE)


def db_close(table):
    pager = table.get("pager")
    num_full_pages = int(table.get("num_rows") / ROWS_PER_PAGE)

    for i in range(0, num_full_pages):
        if pager.get("pages")[i] is None:
            pass
        
        pager_flush(pager, i, PAGE_SIZE)
        #free(pager->pages[i]);
        #pager.get("pages")[i] = None

        # There may be a partial page to write to the end of the file
        # This should not be needed after we switch to a B-tree

    num_additional_rows = table.get("num_rows") % ROWS_PER_PAGE

    if num_additional_rows > 0:
        page_num = num_full_pages
        if pager.get("pages")[page_num] is not None:
            pager_flush(pager, page_num, num_additional_rows * ROW_SIZE)
            # free(pager->pages[page_num])
            # pager.get("pages")[page_num] = None

    try:
        os.close(pager.get("file_descriptor"))
    except os.error as e:
        print("Error closing db file.")
        sys.exit(EXIT_FAILURE)

    # for i in range(0, TABLE_MAX_PAGES):
    #     page = pager.get("pages")[i]
    #     if page:
    #         free(page)
    #         pager.get(pages)[i] = None

    # free(pager)
    # free(table)


# def new_table():
#     table = {
#         "num_rows": 0,
#         "pages": [] #[None] * TABLE_MAX_PAGES
#     }

#     return table

def decode_page(page_bytes):
    print(f"decode_page: '{page_bytes}'")
    page_string = page_bytes.decode("utf-8")
    print(f"page_string: '{page_string}'")
    
    page = []
    
    rows = int(len(page_string) / ROW_SIZE)
    
    for i in range(0, rows):
        match = re.match(r'(.{4})(.{32})(.{255})', page_string[i * ROW_SIZE:], re.IGNORECASE)
    
        if match:
            page.append([int(match.group(1)), match.group(2).strip(), match.group(3).strip()])
        else:
            print("UNEXPECTED format in DB file.")
            sys.exit(2)
    
    return page

def get_page(pager, page_num):
    page_num = int(page_num)

    if page_num > TABLE_MAX_PAGES:
        print(f"Tried to fetch page number out of bounds. {page_num} > {TABLE_MAX_PAGES}")
        sys.exit(EXIT_FAILURE)

    page_in_mem = pager.get("pages")[page_num]
    
    if page_in_mem is None: #or not page_in_mem:
        # Cache miss. Allocate memory and load from file.
        page = []
        num_pages = pager["file_length"] / PAGE_SIZE

        # We might save a partial page at the end of the file
        if pager["file_length"] % PAGE_SIZE == 0:
            num_pages += 1

        # page = None

        if page_num <= num_pages:
            fd = pager.get("file_descriptor")
            os.lseek(fd, page_num * PAGE_SIZE, os.SEEK_SET)
            
            try:
                page_bytes = os.read(fd, PAGE_SIZE)
                page = decode_page(page_bytes)
            except os.error as e:
                print("Error reading file: {}".format(e.errno))
                sys.exit(EXIT_FAILURE)

        pager.get("pages")[page_num] = page

    return pager.get("pages")[page_num]


def row_slot(table, row_num):    
    page_num = row_num / ROWS_PER_PAGE
    page = get_page(table["pager"], page_num)
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


def process_meta_command(cmd, table):
    ret = {
       "success": False,
       "error": ""
    }

    if cmd == ".exit":
        print("That's all folks!")
        db_close(table)
        sys.exit(EXIT_SUCCESS)
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
        match = re.match(r'insert ([+-]?\d+) (.+) (.+)', cmd, re.IGNORECASE)
        if match:
            ret["statement"]["type"] = STATEMENT_INSERT
            
            #print(">>> {} ".format(ret.get("statement")))

            user_id = int(match.group(1))
            username = match.group(2)
            email = match.group(3)
            
            #print(">>> {} {} {}".format(user_id, username, email))
            
            if user_id < 0: # Could use regex
                ret["error"] = PREPARE_NEGATIVE_ID
            elif len(username) > COLUMN_USERNAME_SIZE:
                ret["error"] = PREPARE_STRING_TOO_LONG
            elif len(email) > COLUMN_EMAIL_SIZE:
                ret["error"] = PREPARE_STRING_TOO_LONG
            else:
                ret["success"] = True
                ret["statement"]["row_to_insert"] = [user_id, username, email]
                ret["error"] = ""
        else:
            ret["error"] = PREPARE_SYNTAX_ERROR
    elif cmd.lower().startswith("select"):
        ret["success"] = True
        ret["statement"]["type"] = STATEMENT_SELECT
        ret["error"] = ""

    return ret


def execute_insert(statement, table):
    if table["num_rows"] >= TABLE_MAX_ROWS:
        return EXECUTE_TABLE_FULL
    
    #Row* row_to_insert = &(statement->row_to_insert)
    page_num, index = row_slot(table, table["num_rows"])
    
    print(table)
    page = table.get("pager").get("pages")[page_num]
    page.append(statement["statement"]["row_to_insert"])
    
    table["num_rows"] += 1

    return EXECUTE_SUCCESS


def execute_select(statement, table):
    row = ""
    
    #print(table)
    for i in range(0, int(table.get("num_rows"))):
        page_num, index = row_slot(table, i)
        #print(f"page_num: {page_num}, index: {index}")
        #print(f"page_num: {page_num}, index: {index}, table: {table}")
        #print(table.get("pager").get("pages")[page_num])
        #deserialize_row
        #print(table)
        print_row(table.get("pager").get("pages")[page_num][index])

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
    if len(sys.argv) < 2:
        print("Must supply a database filename.")
        sys.exit(EXIT_FAILURE)

    filename = sys.argv[1]
    table = db_open(filename)

    while True:
        cmd = input("db > ")
        
        if is_meta(cmd):
            ret = process_meta_command(cmd, table)
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
