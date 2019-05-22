from __future__ import print_function

import argparse
import json
import hashlib
import logging
import mimetypes
import os
import os.path
import sys
import uuid

from lxml import etree
from owslib.wps import WebProcessingService, ComplexDataInput, monitorExecution
import requests
import six.moves.urllib.parse as urlparse
from six import StringIO

from galaxy import util


LOGFILE = 'logfile.log'


class StorageHub:
    def __init__(self, gcube_token):
        self.gcube_token = gcube_token
        self.workspace_url = None
        self.galaxy_folder_name = 'Galaxy-DataMiner'
        self.call_id = str(uuid.uuid4())

    def get_base_url(self):
        if self.workspace_url:
            return self.workspace_url
        url = ('http://registry.d4science.org/icproxy/gcube/service/'
               'GCoreEndpoint/DataAccess/StorageHub')
        r = requests.get(url, params={'gcube-token': self.gcube_token})
        r.raise_for_status()
        root = etree.fromstring(r.text)
        endpoints = root.findall('Result/Resource/Profile/AccessPoint/'
                                 'RunningInstanceInterfaces/Endpoint')
        for child in endpoints:
            entry_name = child.attrib['EntryName']
            if entry_name == 'org.gcube.data.access.storagehub.StorageHub':
                return child.text
        return None

    def create_galaxy_folder(self):
        base_url = self.get_base_url()
        # 1. Get id of root folder
        r = requests.get(base_url, params={'gcube-token': self.gcube_token})
        root_id = r.json()['item']['id']
        # 2. Find the Galaxy-DataMiner folder
        r = requests.get(base_url + '/items/%s/children' % root_id,
                         params={'gcube-token': self.gcube_token})
        for folder in r.json()['itemlist']:
            if folder['name'] == self.galaxy_folder_name:
                self.folder_id = folder['id']
                break
        else:
            # folder was not there, create it
            r = requests.post(base_url + '/items/%s/create/FOLDER' % root_id,
                              params={'gcube-token': self.gcube_token},
                              data={'name': self.galaxy_folder_name,
                                    'description': 'A folder to collect Galaxy inputs for DataMiner',
                                    'hidden': False})
            if r.status_code == 200:
                self.folder_id = r.text
            else:
                raise Exception("Cannot create Galaxy folder")

    def upload_file(self, input_name, fname):
        base_url = self.get_base_url()
        files = {
            'name': StringIO('%s-%s' % (input_name, self.call_id)),
            'file': open(fname, 'rb'),
            'description': StringIO('Input %s for DataMiner execution' % input_name)
        }
        r = requests.post(base_url + '/items/%s/create/FILE' % self.folder_id,
                          params={'gcube-token': self.gcube_token}, files=files)
        r.raise_for_status()
        file_id = r.text
        r = requests.get(base_url + '/items/%s/publiclink' % file_id,
                         params={'gcube-token': self.gcube_token})
        r.raise_for_status()
        # D4Science returns the id with quotes :(
        return r.text.strip('"')


def build_inputs(process, text_in, data_in, gcube_token):
    # build a dict to ease input handling later on
    process_inputs = {}
    for i in process.dataInputs:
        process_inputs[i.identifier] = i

    inputs = []
    if text_in:
        for arg in text_in:
            k, v = arg.split('=', 1)
            if not v:
                # skip those not specified, hopefully there will be some sane default
                continue
            clean_v = util.restore_text(v)
            inp = process_inputs.get(k, None)
            if inp:
                if inp.dataType == 'ComplexData':
                    # assume text/xml is fine always?
                    inputs.append((k, ComplexDataInput(clean_v, mimeType='text/xml')))
                else:
                    # let's assume just taking the value is ok
                    inputs.append((k, clean_v))
            else:
                # shouldn't happen
                pass
    if data_in:
        sh = StorageHub(gcube_token)
        sh.create_galaxy_folder()
        for arg in data_in:
            k, v = arg.split('=', 1)
            if not v:
                # skip those not specified, hopefully there will be some sane default
                continue
            clean_v = util.restore_text(v)
            inp = process_inputs.get(k, None)
            if inp:
                if inp.dataType != 'ComplexData':
                    # something went wrong, just abort
                    raise Exception("Data input used for non ComplexData!?")
                # Get things ready in the VRE
                # 1. Copy file to VRE
                file_url = sh.upload_file(k, clean_v)
                # 2. Use file URL for Dataminer
                # assume text/xml is fine always?
                inputs.append((k, ComplexDataInput(file_url, mimeType='text/xml')))
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

    output_dict = { "outputs": []}
    if execution.status == 'ProcessSucceeded':
        html.append('<h2>Outputs:</h2>')
        html.append('<ul>')
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
                output_dict['outputs'].append(
                    {'name': file_name,
                     'mime_type': mime_type.text,
                     'descriptor': desc.text,
                     'url': data.text}
                )
        html.append('</ul>')
    else:
        html.append('<h2>Error:</h2>')
        html.append('<ul>')
        logging.error('Something went wrong:')
        for e in execution.errors:
            html.append('<li><pre>%s</pre></li>' % e.text)
            logging.error(e.text)
        html.append('</ul>')

    html.append('<h2>Execution details:</h2><ul>')
    html.append('<li>Status: %s</li>' % execution.status)
    html.append('<li>ID: %s</li>' % exec_id)
    html.append('<li><a href="%s">WPS log</a></li>' % LOGFILE)
    html.append('</ul>')
    html.append('<script type="application/json" id="output">')
    html.append(json.dumps(output_dict))
    html.append('</script>')
    html.append('</body></html>')
    if outfile:
        with open(outfile, 'w') as ofile:
            ofile.write(''.join(html))


def call_wps(args):
    if not args.token:
        user_id = args.user
        if not user_id:
            logging.error("No user id found on the call, aborting!")
            sys.exit(1)
        user_token_file = os.path.join('/etc/d4science/', user_id)
        with open(user_token_file, 'r') as f:
            gcube_vre_token = f.read()
    else:
        gcube_vre_token = args.token.encode('utf-8')

    logging.info("User: %s", args.user)
    logging.info("Token: (SHA256) %s", hashlib.sha256(gcube_vre_token).hexdigest())

    gcube_vre_token_header = {'gcube-token': gcube_vre_token}

    dataminer_url = ('http://dataminer-prototypes.d4science.org/wps/'
                     'WebProcessingService')
    wps = WebProcessingService(dataminer_url, headers=gcube_vre_token_header)
    process_id = args.process
    process = wps.describeprocess(process_id)

    inputs = build_inputs(process, args.input, args.inputdata, gcube_vre_token)
    outputs = [(o.identifier, True) for o in process.processOutputs]
    # execute the process
    execution = wps.execute(process_id, inputs, outputs)
    monitorExecution(execution, sleepSecs=5, download=True)
    logging.info("Execution status: %s", execution.status)
    exit_code = 0 if execution.status == 'ProcessSucceeded' else 1
    logging.info("Exit code: %d", exit_code)
    produce_output(execution, args.output, args.outdir, gcube_vre_token_header)
    return exit_code


def main():
    parser = argparse.ArgumentParser(description='Call the DataMiner processes')
    parser.add_argument('--process', help='id of the process')
    parser.add_argument('--input', action='append',
                        help='input parameter')
    parser.add_argument('--inputdata', action='append',
                        help='input parameter (as Galaxy data)')
    parser.add_argument('--output', help='output html file')
    parser.add_argument('--outdir', help='output directory')
    parser.add_argument('--user', help='user')
    parser.add_argument('--token', help='gcube-token')

    args = parser.parse_args()

    if not os.path.exists(args.outdir):
        os.makedirs(args.outdir)
    logging.basicConfig(level=logging.DEBUG,
                        filename=os.path.join(args.outdir, LOGFILE))
    log_error = logging.StreamHandler(sys.stderr)
    log_out = logging.StreamHandler(sys.stdout)
    log_error.setLevel(logging.ERROR)
    log_out.setLevel(logging.ERROR)

    logging.getLogger('').addHandler(log_error)
    logging.getLogger('').addHandler(log_out)

    logging.debug("Arguments:")
    logging.debug("Process: %s", args.process)
    if args.input:
        logging.debug("Input: %s", ' '.join(args.input))
    if args.inputdata:
        logging.debug("Input data: %s", ' '.join(args.inputdata))
    logging.debug("Output: %s", args.output)
    logging.debug("Outdir: %s", args.outdir)
    logging.debug("User: %s", args.user)
    if args.token:
        logging.debug("Token: (SHA256) %s",
                      hashlib.sha256(args.token.encode('utf-8')).hexdigest())

    exit_code = call_wps(args)
    if exit_code != 0:
        logging.error("Error on wps execution!")
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
