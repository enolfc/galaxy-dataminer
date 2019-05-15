import argparse
import json
import os
import os.path
import shutil

from html.parser import HTMLParser

class DataMinerHTMLParser(HTMLParser):
    def __init__(self):
        self.extract_data = False
        self.output_files = {}
        super().__init__()

    def handle_starttag(self, tag, attrs):
        if tag == 'script':
            ad = dict(attrs)
            if ad.get('type') == 'application/json' and ad.get('id') == 'output':
                self.extract_data = True

    def handle_endtag(self, tag):
        self.extract_data = False

    def handle_data(self, data):
        if self.extract_data:
            self.output_files = json.loads(data)


def main():
    arg_parser = argparse.ArgumentParser(description='Get some Dataminer output into Galaxy')
    arg_parser.add_argument('--inputdata',
                            help='Galaxy dataset coming from dataminer execution')
    arg_parser.add_argument('--descriptor', help='File to get from the output')
    arg_parser.add_argument('--output', help='output file')

    args = arg_parser.parse_args()
    html_parser = DataMinerHTMLParser()

    with open(os.path.join(args.inputdata), 'r') as f:
        html_parser.feed(f.read())

    outfiles = html_parser.output_files

    inputdir = os.path.dirname(args.inputdata)
    if args.descriptor:
        for f in outfiles['outputs']:
            if f['descriptor'] == args.descriptor:
                src = os.path.join(inputdir, f['name'])
                shutil.copyfile(src, args.output)
                break
        else:
            raise Exception("Output not found")
    else:
        for f in outfiles['outputs']:
            # just do not use the "Log of the Computation"
            if f['mime_type'] == 'text/csv' and f['descriptor'] != 'Log of the computation':
                src = os.path.join(inputdir, f['name'])
                shutil.copyfile(src, args.output)
                break
        else:
            raise Exception("Output not found")

if __name__ == '__main__':
    main()
