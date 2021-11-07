import json
import sys

def __main__():
    if sys.argv[1] == "create":
        config_file = open(sys.argv[2])
        configuration = json.load(config_file)
        print(configuration) 

__main__()