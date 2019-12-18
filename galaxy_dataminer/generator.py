from __future__ import print_function

import argparse
import logging
import os
import shutil
import sys

import requests
from owslib.wps import WebProcessingService, ComplexDataInput, monitorExecution
from owslib.wps import printInputOutput
from lxml import etree
from xml.dom import minidom


def complex_data_input(input_attrs):
    cond = etree.Element("conditional", attrib={"name": input_attrs["name"]})
    param_attrs = {
        "name": "input_type_selector" % input_attrs,
        "type": "select",
        "label": "Choose the type of input",
    }
    select = etree.SubElement(cond, "param", attrib=param_attrs)
    etree.SubElement(
        select, "option", attrib={"value": "dataset", "selected": "true"}
    ).text = "Use a Galaxy dataset"
    etree.SubElement(select, "option", attrib={"value": "URL"}).text = "Specify URL"
    when_dataset = etree.SubElement(cond, "when", attrib={"value": "dataset"})
    dataset_attrs = input_attrs.copy()
    dataset_attrs.update({"name": "data", "type": "data"})
    etree.SubElement(when_dataset, "param", attrib=dataset_attrs)
    when_url = etree.SubElement(cond, "when", attrib={"value": "URL"})
    url_attrs = input_attrs.copy()
    url_attrs.update({"name": "data"})
    etree.SubElement(when_url, "param", attrib=url_attrs)
    return cond


def generate_tool_description(process, descr, tool_file):
    tool_attrs = {
        "version": process.processVersion,
        "id": "d4science:%s" % process.identifier,
        "name": descr.title,
    }
    tool = etree.Element("tool", attrib=tool_attrs)
    etree.SubElement(tool, "description").text = descr.abstract
    cmd = etree.SubElement(tool, "command", attrib={"interpreter": "sh"})
    cmd_line = ["call-wps.sh", "--process", process.identifier]
    inputs = etree.SubElement(tool, "inputs")
    for inp in descr.dataInputs:
        input_attrs = {
            "name": inp.identifier,
            "label": inp.identifier,
            "type": "text",
            "help": inp.title,
        }
        if inp.dataType == "ComplexData":
            inputs.append(complex_data_input(input_attrs))
            cmd_line.append(
                "#if str($%(name)s.input_type_selector)" ' == "URL":' % input_attrs
            )
            cmd_line.append("--input '%(name)s=$%(name)s.data'" % input_attrs)
            cmd_line.append("#else:")
            cmd_line.append("--inputdata '%(name)s=$%(name)s.data'" % input_attrs)
            cmd_line.append("#end if")
        else:
            if inp.defaultValue and inp.dataType != "ComplexData":
                input_attrs["value"] = inp.defaultValue
            param = etree.SubElement(inputs, "param", attrib=input_attrs)
            cmd_line.append("--input '%(name)s=$%(name)s'" % input_attrs)
    cmd_line.append("--output $html_file --outdir $html_file.files_path")
    cmd_line.append("--user $__user_email__")
    cmd.text = etree.CDATA("\n".join(cmd_line))
    outputs = etree.SubElement(tool, "outputs")
    for o in descr.processOutputs:
        printInputOutput(o)
        output_attrs = {
            "format": "html",
            "name": "html_file",
            "label": "Dataminer output for %s" % descr.title,
        }
        etree.SubElement(outputs, "data", attrib=output_attrs)
    etree.SubElement(tool, "help").text = descr.abstract
    xmlstr = minidom.parseString(etree.tostring(tool)).toprettyxml(indent="  ")
    with open(tool_file, "wb") as f:
        f.write(xmlstr.encode("utf-8"))


def find_section(config, section_id):
    root = config.getroot()
    sections = root.findall("section")
    for s in sections:
        if s.attrib.get("id", "") == section_id:
            s.clear()
            s.set("id", section_id)
            s.set("name", "DataMiner")
            return s
    # no d4science section, so creating one
    s = etree.Element("section", attrib={"name": "DataMiner", "id": section_id})
    root.insert(0, s)
    return s


def fill_section(section, gcube_vre_token, tool_dir):
    gcube_vre_token_header = {"gcube-token": gcube_vre_token}

    dataminer_url = (
        "http://dataminer-prototypes.d4science.org/wps/" "WebProcessingService"
    )
    wps = WebProcessingService(dataminer_url, headers=gcube_vre_token_header)

    tools = {}
    tools["CSV extractor"] = {"file": os.path.join(tool_dir, "extract.xml")}
    for process in wps.processes:
        descr = wps.describeprocess(process.identifier)
        tools[descr.title] = {"descr": descr, "process": process, "file": None}

    for i, t in enumerate(sorted(tools)):
        tool_file = os.path.join(tool_dir, "tool%02d.xml" % i)
        if tools[t]["file"]:
            shutil.copyfile(tools[t]["file"], tool_file)
        else:
            generate_tool_description(tools[t]["process"], tools[t]["descr"], tool_file)
        etree.SubElement(section, "tool", attrib={"file": tool_file})


def main():
    parser = argparse.ArgumentParser(description="Build dataminer tools")
    parser.add_argument("--config", help="config file for galaxy tools")
    parser.add_argument("--token", help="file with d4science token for the VRE")
    parser.add_argument(
        "--section", default="d4science", help="name of the d4science section"
    )
    parser.add_argument("--outdir", help="tools configuration directory")

    args = parser.parse_args()
    if not os.path.exists(args.outdir):
        os.makedirs(args.outdir)

    logging.basicConfig(level=logging.ERROR)

    config = etree.parse(args.config)
    root = config.getroot()

    with open(args.token, "r") as f:
        token = f.read().strip()

    d4science_config = find_section(config, args.section)
    fill_section(d4science_config, token, args.outdir)

    xmlstr = minidom.parseString(etree.tostring(root)).toprettyxml(indent="  ")
    print(xmlstr.encode("utf-8"))


if __name__ == "__main__":
    main()
