import argparse
import json
import os
import os.path
import shutil

from galaxy_dataminer.caller import OUTDESC


def main():
    parser = argparse.ArgumentParser(description='Get some Dataminer output into Galaxy')
    parser.add_argument('--inputdir', help='directory where the output of '
                                           'previous execution is available')
    parser.add_argument('--descriptor', help='File to get from the output')
    parser.add_argument('--output', help='output file')

    args = parser.parse_args()

    with open(os.path.join(args.inputdir, OUTDESC), 'r') as descriptor:
        outfiles = json.loads(descriptor.read())
    if args.descriptor:
        for f in outfiles['outputs']:
            if f['descriptor'] == args.descriptor:
                src = os.path.join(args.inputdir, f['name'])
                shutil.copyfile(src, args.output)
                break
        else:
            raise Exception("Output not found")
    else:
        for f in outfiles['outputs']:
            # just do not use the "Log of the Computation"
            if f['mime_type'] == 'text/csv' and f['descriptor'] != 'Log of the computation':
                src = os.path.join(args.inputdir, f['name'])
                shutil.copyfile(src, args.output)
                break
        else:
            raise Exception("Output not found")

if __name__ == '__main__':
    main()
