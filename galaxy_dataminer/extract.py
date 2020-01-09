import argparse
import json
import os
import os.path
import shutil

from galaxy_dataminer.caller_parser import CallerHTMLParser

def main():
    arg_parser = argparse.ArgumentParser(
        description="Get some Dataminer output into Galaxy"
    )
    arg_parser.add_argument(
        "--inputdata", help="Galaxy dataset coming from dataminer execution"
    )
    arg_parser.add_argument("--inputdir", help="Extra files path for input")
    arg_parser.add_argument("--descriptor", help="File to get from the output")
    arg_parser.add_argument("--output", help="output file")

    args = arg_parser.parse_args()
    html_parser = CallerHTMLParser()

    with open(os.path.join(args.inputdata), "r") as f:
        html_parser.feed(f.read())

    outfiles = html_parser.caller_dataminer_data().get("outputs", [])

    if args.descriptor:
        for f in outfiles["outputs"]:
            if f["descriptor"] == args.descriptor:
                src = os.path.join(args.inputdir, f["name"])
                shutil.copyfile(src, args.output)
                break
        else:
            raise Exception("Output not found")
    else:
        for f in outfiles["outputs"]:
            # do not use 'Log of the computation'
            if (
                f["mime_type"] == "text/csv"
                and f["descriptor"] != "Log of the computation"
            ):
                src = os.path.join(args.inputdir, f["name"])
                shutil.copyfile(src, args.output)
                break
        else:
            raise Exception("Output not found")


if __name__ == "__main__":
    main()
