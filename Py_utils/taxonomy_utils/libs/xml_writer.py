import sys, os, inspect, re
import time, logging
from jinja2 import Environment, FileSystemLoader
from lxml import etree
from lxml.etree import _ElementTree, _Element, XMLParser
from io import StringIO, BytesIO
from libs import path_resolver

template_file_name = 'audit_lmt_ds_schema_config.xml'


def generate_output_config(create_tg_schema_obj_list=[], drop_tg_schema_obj_list=[],
                           exposed_tg_name_ordered_list=[],
                           dn_version='12.0', dest:str='', file_name:str='', 
                           print_oc_content = True, dry_run = False):
    log = logging.getLogger(__name__)
    xml = get_xml_template(template_file_name, 
                           dn_version = dn_version, 
                           create_tg_schema_obj_list=create_tg_schema_obj_list, 
                           drop_tg_schema_obj_list = drop_tg_schema_obj_list, 
                           exposed_tg_name_ordered_list=exposed_tg_name_ordered_list)
    
    if print_oc_content:
        log.info("\n" + xml)
    
    root_ele = etree.fromstring(xml)
    if dry_run:
        return True
    return write_xml(root_ele, dest, file_name)

def get_xml_template(template_file_name, **kwargs):

    path = os.path.join("templates", "xml") 
    templates_dir = path_resolver.resolve(path)
    env = Environment( loader = FileSystemLoader(templates_dir) )
    template = env.get_template(template_file_name)
    return template.render(**kwargs)


def write_xml(root_element :_Element, dest : str, file_name : str):
    log = logging.getLogger(__name__)
    if not os.path.exists(dest):
        log.error('Destination path {} is not valid'.format(dest))
        raise Exception("Not a valid destination")
        #os.makedirs(dest)
    destpath: Path = os.path.join(dest, file_name)
    valid: bool
    error: str
    valid, error = __write_xml(root_element, destpath)
    if not valid:
        log.warnings.warn('Could not write xml file\
            "{}"'.format(error))
        return False
    log.debug("desting location: %s", destpath)
    log.info("%s has been generated.\n\n\n", file_name)
    return True
	

def __write_xml(tree: _Element, path: str) -> tuple:
    """
    Write dictionary structure into xml file 
    """
    log = logging.getLogger(__name__)
    if tree is None:
        return (False, 'No dictionary provided to write to xml')

    if path is None:
        return (False, 'No xml path provided to write dictionary to')

    try:
        with open(path, mode='wb') as fp:
            # Format the output while writing. Note that
            # any initial indendation has to be removed
            # while reading from source file/string.
            eetree = etree.ElementTree(tree)
            eetree.write(fp, pretty_print=True, xml_declaration=True,
                         encoding='utf-8', method="xml")
        return (True, None)
    except ParseError:
        error: str = 'Error in xml encoding while writing to file "{}"'.format(
            path)
        log.warnings.warn(error)
        return (False, error)
    except OSError:
        error: str = 'Error in writing xml to file "{}"'.format(path)
        log.warnings.warn(error)
        return (False, error)