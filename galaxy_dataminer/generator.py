from __future__ import print_function

import argparse
import logging
import os
import sys

import requests
from owslib.wps import WebProcessingService, ComplexDataInput, monitorExecution
import xml.etree.ElementTree as etree
from xml.dom import minidom


def generate_tool_description(process, descr, tool_file):
    tool_attrs = {
        'version': process.processVersion,
        'id': 'd4science:%s' % process.identifier,
        'name': descr.title,
    }
    tool = etree.Element('tool', attrib=tool_attrs)
    etree.SubElement(tool, 'description').text = descr.abstract
    cmd = etree.SubElement(tool, 'command', attrib={'interpreter': 'shell'})
    cmd_line = ['call-wps.sh', '--process', process.identifier]
    inputs = etree.SubElement(tool, 'inputs')
    for inp in descr.dataInputs:
        input_attrs = {
            'name': inp.identifier,
            'label': inp.identifier,
            'type': 'text',
            'help': inp.title
        }
        if inp.defaultValue and inp.dataType != 'ComplexData':
            input_attrs['value'] = inp.defaultValue
        param = etree.SubElement(inputs, 'param', attrib=input_attrs)
        cmd_line.append("--input %(name)s=$%(name)s" % input_attrs)
    cmd_line.append('--output $html_file --outdir $html_file.files_path')
    cmd_line.append('--user $__user_email__')
    cmd.text = ' '.join(cmd_line)
    outputs = etree.SubElement(tool, 'outputs')
    for o in descr.processOutputs:
        output_attrs = {
            'format': 'html',
            'name': 'html_file',
            'label': 'Dataminer output for %s' % descr.title,
        }
        etree.SubElement(outputs, 'data', attrib=output_attrs)
    etree.SubElement(tool, 'help').text = descr.abstract
    xmlstr = minidom.parseString(etree.tostring(tool)).toprettyxml(indent="  ")
    with open(tool_file, "w") as f:
        f.write(xmlstr.encode('utf-8'))

def find_section(config, section_id):
    root = config.getroot()
    sections = root.findall('section')
    for s in sections:
        if s.attrib.get('id', '') == section_id:
            s.clear()
            s.set('id', section_id)
            s.set('name', 'DataMiner')
            return s
    # no d4science section, so creating one
    s = etree.Element('section',
		      attrib={'name': 'DataMiner', 'id': section_id})
    root.insert(0, s)
    return s


def fill_section(section, gcube_vre_token, tool_dir):
    gcube_vre_token_header = {'gcube-token': gcube_vre_token}


    dataminer_url = ('http://dataminer-prototypes.d4science.org/wps/'
                     'WebProcessingService')
    wps = WebProcessingService(dataminer_url, headers=gcube_vre_token_header)

    for i, process in enumerate(wps.processes):
        tool_file = os.path.join(tool_dir, 'tool%s.xml' % i) 
        descr = wps.describeprocess(process.identifier)
        generate_tool_description(process, descr, tool_file)
        etree.SubElement(section, 'tool', attrib={'file': tool_file})


def main():
    parser = argparse.ArgumentParser(description='Build dataminer tools')
    parser.add_argument('--config', help='config file for galaxy tools')
    parser.add_argument('--token', help='d4science token for the VRE')
    parser.add_argument('--section', default='d4science',
                        help='name of the d4science section')
    parser.add_argument('--outdir', help='tools configuration directory')

    args = parser.parse_args()
    if not os.path.exists(args.outdir):
        os.makedirs(args.outdir)

    logging.basicConfig(level=logging.ERROR)

    config = etree.parse(args.config)
    root = config.getroot()

    d4science_config = find_section(config, args.section)
    fill_section(d4science_config, args.token, args.outdir)

    xmlstr = minidom.parseString(etree.tostring(root)).toprettyxml(indent="  ")
    print(xmlstr.encode('utf-8'))


if __name__ == '__main__':
    main()
