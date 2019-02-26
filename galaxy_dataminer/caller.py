from __future__ import print_function

import argparse
import json
import hashlib
import logging
import mimetypes
import os
import os.path
import sys

from owslib.wps import WebProcessingService, ComplexDataInput, monitorExecution
import requests
import six.moves.urllib.parse as urlparse
import xml.etree.ElementTree as etree


LOGFILE_NAME= 'logfile.log'


def build_inputs(process, args_inputs):
    # build a dict to ease input handling later on
    process_inputs = {}
    for i in process.dataInputs:
        process_inputs[i.identifier] = i

    inputs = []
    if args_inputs:
        for arg in args_inputs:
            k, v = arg.split('=', 1)
            if not v:
                # skip those not specified, hopefully there will be some sane default
                continue
            inp = process_inputs.get(k, None)
            if inp:
                if inp.dataType == 'ComplexData':
                    # assume text/xml is fine always?
                    inputs.append((k, ComplexDataInput(v, mimeType='text/xml')))
                else:
                    # let's assume just taking the value is ok
                    inputs.append((k, v))
            else:
                # shouldn't happen
                pass
    return inputs


def produce_output(execution, outfile, outdir, gcube_vre_token_header):
    # Build some simple HTML output with the links to the actual output
    html = ['<html><body><h1>DataMiner algorithm: %s</h1>'
            % execution.process.title]

    exec_id = ''
    status_url = urlparse.urlparse(execution.statusLocation)
    if status_url[4]:
        exec_id = urlparse.parse_qs(status_url[4]).get('id', '')
        if exec_id:
            exec_id = exec_id.pop()

    if execution.status == 'ProcessSucceeded':
        html.append('<h2>Outputs:</h2>')
        html.append('<ul>')
        output_dict = { "outputs": []}
        for out in execution.processOutputs:
            if not out.fileName:
                continue
            tree = etree.parse(out.fileName)
            featMembers = tree.findall('{http://www.opengis.net/gml}featureMember')
            results = featMembers[0].findall('{http://ogr.maptools.org/}Result')

            for result in results:
                data = result.find('{http://www.d4science.org}Data')
                mime_type = result.find('{http://www.d4science.org}MimeType')
                extension = mimetypes.guess_extension(mime_type.text)
                if not extension:
                    extension = ''
                desc = result.find('{http://www.d4science.org}Description')
                r = requests.get(data.text, stream=True,
                                 headers=gcube_vre_token_header)

                # Throw an error for bad status codes
                r.raise_for_status()

                file_name = '%s%s' % (desc.text, extension)
                with open(os.path.join(outdir, file_name), 'wb') as handle:
                    for block in r.iter_content(1024):
                        handle.write(block)
                html.append('<li><a href="%s">%s</a></li>'
                            % (file_name, desc.text))
                output_dict['outputs'].append({'name': file_name, 'mime_type': mime_type.text})
        html.append('</ul>')
        with open(os.path.join(outdir, 'wps-out.json'), 'w') as descriptor:
            descriptor.write(json.dumps(output_dict))
    else:
        html.append('<h2>Error:</h2>')
        html.append('<ul>')
        for e in execution.errors:
            html.append('<li><pre>%s</pre></li>' % e.text)
        html.append('</ul>')

    html.append('<h2>Execution details:</h2><ul>')
    html.append('<li>Status: %s</li>' % execution.status)
    html.append('<li>ID: %s</li>' % exec_id)
    html.append('<li><a href="%s">WPS log</a></li>' % LOGFILE_NAME)
    html.append('</ul>')
    html.append('</body></html>')
    if outfile:
        with open(outfile, 'w') as ofile:
            ofile.write(''.join(html))


def call_wps(args):
    user_id = args.user
    if not user_id:
        logging.error("No user id found on the call, aborting!")
        sys.exit(1)
    user_token_file = os.path.join('/etc/d4science/', user_id)
    with open(user_token_file, 'r') as f:
        gcube_vre_token = f.read()

    logging.info("User: %s", args.user)
    logging.info("Token: (SHA256) %s", hashlib.sha256(gcube_vre_token).hexdigest())

    gcube_vre_token_header = {'gcube-token': gcube_vre_token}

    dataminer_url = ('http://dataminer-prototypes.d4science.org/wps/'
                     'WebProcessingService')
    wps = WebProcessingService(dataminer_url, headers=gcube_vre_token_header)
    process_id = args.process
    process = wps.describeprocess(process_id)

    inputs = build_inputs(process, args.input)
    outputs = [(o.identifier, True) for o in process.processOutputs]
    # execute the process
    execution = wps.execute(process_id, inputs, outputs)
    monitorExecution(execution, sleepSecs=5, download=True)
    logging.info("Execution status: %s", execution.status)
    exit_code = 0 if execution.status == 'ProcessSucceded' else 1
    logging.info("Exit code: %d", exit_code)
    produce_output(execution, args.output, args.outdir, gcube_vre_token_header)
    return exit_code


def main():
    parser = argparse.ArgumentParser(description='Call the DataMiner processes')
    parser.add_argument('--process', help='id of the process')
    parser.add_argument('--input', action='append',
                        help='input parameter)')
    parser.add_argument('--output', help='output html file')
    parser.add_argument('--outdir', help='output directory')
    parser.add_argument('--user', help='user')

    args = parser.parse_args()

    if not os.path.exists(args.outdir):
        os.makedirs(args.outdir)
    logging.basicConfig(level=logging.DEBUG,
                        filename=os.path.join(args.outdir, LOGFILE_NAME))
    logging.debug("Arguments:")
    logging.debug("Process: %s", args.process)
    logging.debug("Input: %s", ' '.join(args.input))
    logging.debug("Output: %s", args.output)
    logging.debug("Outdir: %s", args.outdir)
    logging.debug("User: %s", args.user)

    sys.exit(call_wps(args))

if __name__ == '__main__':
    main()
