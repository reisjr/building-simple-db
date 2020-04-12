import os
import sys

#def print_prompt():
EXIT_SUCCESS = 0
STATEMENT_INSERT = 0
STATEMENT_SELECT = 1

def is_meta(cmd):
    ret = False

    if cmd.startswith("."):
        ret = True

    return ret


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
            "type": ""
        }
    }

    if cmd.lower().startswith("insert"):
        ret["success"] = True
        ret["statement"]["type"] = STATEMENT_INSERT
    elif cmd.lower().startswith("select"):
        ret["success"] = True
        ret["statement"]["type"] = STATEMENT_SELECT

    return ret


def execute_statement(statement):
    print(f"Executing statement '{statement}'.")
    
    if statement["statement"]["type"] == STATEMENT_INSERT:
      print("This is where we would do an insert.")
    elif statement["statement"]["type"] == STATEMENT_SELECT:
      print("This is where we would do a select.")


if __name__ == "__main__":
    cmd = ""

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
                execute_statement(statement)
                print("Executed.")
            else:
                print(f"Unrecognized keyword at start of '{cmd}'.")
