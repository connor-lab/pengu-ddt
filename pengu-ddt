#!/usr/bin/env python3

from penGU.input.parse_args import parse_commandline_args
from penGU.input.utils import check_config_file
from penGU.input.run_command import run_commmand
        

def main():
    args = parse_commandline_args()
    config_dict = check_config_file(args.config_file)
    run_commmand(config_dict, args)
    

if __name__ == "__main__":
    main()

