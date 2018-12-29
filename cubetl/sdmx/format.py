# CubETL

import logging
from cubetl.core import Node
import itertools
from lxml import etree
from cubetl.xml import XmlPullParser
import copy


# Get an instance of a logger
logger = logging.getLogger(__name__)



class SDMXDSD(object):

    def __init__(self, path):

        self.path = path

        self.data = {}

        self.read()

    def read(self):

        parser = etree.XMLParser(ns_clean=True, recover=True, encoding="utf-8")
        dsd = etree.parse(self.path, parser=parser)

        namespaces = {
            "xml": "http://www.w3.org/XML/1998/namespace",
            "structure": "http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure"
        }

        # Read codelists
        self.data['codelists'] = {}
        #print dsd.findall('*/structure:CodeList', namespaces=namespaces)
        for codelist in dsd.findall('*/structure:CodeList', namespaces=namespaces):
            codelist_id = codelist.attrib['id']
            for name in codelist.findall('structure:Name', namespaces=namespaces):
                codelist_text = name.text
                codelist_lang = name.attrib['{http://www.w3.org/XML/1998/namespace}lang']
                if codelist_id not in self.data['codelists']:
                    self.data['codelists'][codelist_id] = { 'id': codelist_id, 'label': codelist_text }
                self.data['codelists'][codelist_id]["label_" + codelist_lang] = codelist_text

                self.data['codelists'][codelist_id]["codes"] = {}
                for code in codelist.findall('structure:Code', namespaces=namespaces):
                    code_value = code.attrib['value']
                    for description in code.findall('structure:Description', namespaces=namespaces):
                        code_text = description.text
                        code_lang = description.attrib['{http://www.w3.org/XML/1998/namespace}lang']
                        if code_value not in self.data['codelists'][codelist_id]["codes"]:
                            self.data['codelists'][codelist_id]["codes"][code_value] = {'value': code_value, 'label': code_text }
                        self.data['codelists'][codelist_id]["codes"][code_value]["label_" + code_lang] = code_text

        self.data['concepts'] = {}
        for concept in dsd.findall('**/structure:Concept', namespaces=namespaces):
            concept_id = concept.attrib['id']
            for name in concept.findall('structure:Name', namespaces=namespaces):
                concept_text = name.text
                concept_lang = name.attrib['{http://www.w3.org/XML/1998/namespace}lang']
                if concept_id not in self.data['concepts']:
                    self.data['concepts'][concept_id] = { 'id': concept_id, 'label': concept_text }
                self.data['concepts'][concept_id]["label_" + concept_lang] = concept_text

        self.data['dimensions'] = {}
        for dimension in dsd.findall('***/structure:Dimension', namespaces=namespaces):
            dimension_id = dimension.attrib['conceptRef']
            dimension_codelist = dimension.attrib['codelist']
            self.data["dimensions"][dimension_id] = {}
            self.data["dimensions"][dimension_id]["concept"] = self.data["concepts"][dimension_id]
            self.data["dimensions"][dimension_id]["codelist"] = self.data["codelists"][dimension_codelist]

        self.data['timedimensions'] = {}
        for dimension in dsd.findall('***/structure:TimeDimension', namespaces=namespaces):
            dimension_id = dimension.attrib['conceptRef']
            self.data["timedimensions"][dimension_id] = {}
            self.data["timedimensions"][dimension_id]["concept"] = self.data["concepts"][dimension_id]

        self.data['measures'] = {}
        for measure in dsd.findall('***/structure:PrimaryMeasure', namespaces=namespaces):
            measure_id = measure.attrib['conceptRef']
            self.data["measures"][measure_id] = { "name": measure_id,
                                                  "label": self.data["concepts"][measure_id]["label"] }


class SDMXData(object):

    def __init__(self, path_dsd, path_sdmx):
        self.path_dsd = path_dsd
        self.path_sdmx = path_sdmx

    def read(self, ctx):

        dsd = SDMXDSD(self.path_dsd)

        self._xmlPullParser = XmlPullParser()
        self._xmlPullParser.path = self.path_sdmx
        self._xmlPullParser.tagname = "data:Series"
        ctx.comp.initialize(self._xmlPullParser)

        sdmxdata = ctx.comp.process(self._xmlPullParser, {})

        namespaces = {
            "data": "",
            "xml": "http://www.w3.org/XML/1998/namespace",
            "structure": "http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure"
        }

        for node in sdmxdata:

            xml = node['xml']


            #for attr, attrval in node.attributes.items():
            #    fact[attr] = attrval

            for series in xml.findall("Series"):

                #fact = { "dsd": dsd }
                fact = {}
                for dimension_key, dimension in dsd.data['dimensions'].items():
                    attr_value = series.attrib[dimension_key]
                    fact[dimension_key + "_code"] = attr_value
                    fact[dimension_key + "_label"] = dimension['codelist']['codes'][attr_value]["label"]

                fact2 = copy.copy(fact)

                for obs in series.findall("Obs"):

                    valid = True

                    for dimension_key, dimension in dsd.data['timedimensions'].items():
                        attr_value = obs.attrib[dimension_key]
                        fact2[dimension_key] = attr_value

                    for measure_key, measure in dsd.data['measures'].items():
                        try:
                            attr_value = obs.attrib[measure_key]
                            fact2[measure_key] = attr_value
                        except KeyError as e:
                            logger.debug("SDMX observation without value. Discarding cell.")
                            valid = False

                    if valid:
                        yield fact2

        ctx.comp.finalize(self._xmlPullParser)

        """
        parser = etree.XMLParser(ns_clean=True, recover=True, encoding="utf-8")
        data = etree.parse(self.path, parser=parser)

        namespaces = {
            "xml": "http://www.w3.org/XML/1998/namespace",
            "structure": "http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure"
        }
        """
