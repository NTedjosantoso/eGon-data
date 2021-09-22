from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql.base import ischema_names
from geoalchemy2 import Geometry

from egon.data.db import engine


def context():
    """
    Project context information for metadata

    Returns
    -------
    dict
        OEP metadata conform data license information
    """

    return {
        "homepage": "https://ego-n.org/",
        "documentation": "https://egon-data.readthedocs.io/en/latest/",
        "sourceCode": "https://github.com/openego/eGon-data",
        "contact": "https://ego-n.org/partners/",
        "grantNo": "03EI1002",
        "fundingAgency": "Bundesministerium für Wirtschaft und Energie",
        "fundingAgencyLogo": "https://www.innovation-beratung-"
        "foerderung.de/INNO/Redaktion/DE/Bilder/"
        "Titelbilder/titel_foerderlogo_bmwi.jpg?"
        "__blob=normal&v=3",
        "publisherLogo": "https://ego-n.org/images/eGon_logo_"
        "noborder_transbg.svg"
    }


def licenses_datenlizenz_deutschland(attribution):
    """
    License information for Datenlizenz Deutschland

    Parameters
    ----------
    attribution : str
        Attribution for the dataset incl. © symbol, e.g. '© GeoBasis-DE / BKG'

    Returns
    -------
    dict
        OEP metadata conform data license information
    """

    return {
        "name": "dl-by-de/2.0",
        "title": "Datenlizenz Deutschland – Namensnennung – Version 2.0",
        "path": "www.govdata.de/dl-de/by-2-0",
        "instruction": (
            "Jede Nutzung ist unter den Bedingungen dieser „Datenlizenz "
            "Deutschland - Namensnennung - Version 2.0 zulässig.\nDie "
            "bereitgestellten Daten und Metadaten dürfen für die "
            "kommerzielle und nicht kommerzielle Nutzung insbesondere:"
            "(1) vervielfältigt, ausgedruckt, präsentiert, verändert, "
            "bearbeitet sowie an Dritte übermittelt werden;\n "
            "(2) mit eigenen Daten und Daten Anderer zusammengeführt und "
            "zu selbständigen neuen Datensätzen verbunden werden;\n "
            "(3) in interne und externe Geschäftsprozesse, Produkte und "
            "Anwendungen in öffentlichen und nicht öffentlichen "
            "elektronischen Netzwerken eingebunden werden.\n"
            "Bei der Nutzung ist sicherzustellen, dass folgende Angaben "
            "als Quellenvermerk enthalten sind:\n"
            "(1) Bezeichnung des Bereitstellers nach dessen Maßgabe,\n"
            "(2) der Vermerk Datenlizenz Deutschland – Namensnennung – "
            "Version 2.0 oder dl-de/by-2-0 mit Verweis auf den Lizenztext "
            "unter www.govdata.de/dl-de/by-2-0 sowie\n"
            "(3) einen Verweis auf den Datensatz (URI)."
            "Dies gilt nur soweit die datenhaltende Stelle die Angaben"
            "(1) bis (3) zum Quellenvermerk bereitstellt.\n"
            "Veränderungen, Bearbeitungen, neue Gestaltungen oder "
            "sonstige Abwandlungen sind im Quellenvermerk mit dem Hinweis "
            "zu versehen, dass die Daten geändert wurden."
        ),
        "attribution": attribution
    }


def license_odbl(attribution):
    """
    License information for Open Data Commons Open Database License (ODbL-1.0)

    Parameters
    ----------
    attribution : str
        Attribution for the dataset incl. © symbol, e.g.
        '© OpenStreetMap contributors'

    Returns
    -------
    dict
        OEP metadata conform data license information
    """
    return {
        "name": "ODbL-1.0",
        "title": "Open Data Commons Open Database License 1.0",
        "path": "https://opendatacommons.org/licenses/odbl/1.0/index.html",
        "instruction": "You are free: To Share, To Create, To Adapt; "
                       "As long as you: Attribute, Share-Alike, Keep open!",
        "attribution": attribution
    }


def license_ccby(attribution):
    """
    License information for Creative Commons Attribution 4.0 International
    (CC-BY-4.0)

    Parameters
    ----------
    attribution : str
        Attribution for the dataset incl. © symbol, e.g. '© GeoBasis-DE / BKG'

    Returns
    -------
    dict
        OEP metadata conform data license information
    """
    return {
        "name": "CC-BY-4.0",
        "title": "Creative Commons Attribution 4.0 International",
        "path": "https://creativecommons.org/licenses/by/4.0/legalcode",
        "instruction": "You are free: To Share, To Create, To Adapt; "
                       "As long as you: Attribute.",
        "attribution": attribution
    }


def license_geonutzv(attribution):
    """
    License information for GeoNutzV

    Parameters
    ----------
    attribution : str
        Attribution for the dataset incl. © symbol, e.g. '© GeoBasis-DE / BKG'

    Returns
    -------
    dict
        OEP metadata conform data license information
    """
    return {
        "name": "geonutzv-de-2013-03-19",
        "title": "Verordnung zur Festlegung der Nutzungsbestimmungen für die "
                 "Bereitstellung von Geodaten des Bundes",
        "path": "https://www.gesetze-im-internet.de/geonutzv/",
        "instruction": "Geodaten und Geodatendienste, einschließlich "
                       "zugehöriger Metadaten, werden für alle derzeit "
                       "bekannten sowie für alle zukünftig bekannten Zwecke "
                       "kommerzieller und nicht kommerzieller Nutzung "
                       "geldleistungsfrei zur Verfügung gestellt, soweit "
                       "durch besondere Rechtsvorschrift nichts anderes "
                       "bestimmt ist oder vertragliche oder gesetzliche "
                       "Rechte Dritter dem nicht entgegenstehen.",
        "attribution": attribution
    }


def generate_resource_fields_from_sqla_model(model):
    """ Generate a template for the resource fields for metadata from a SQL
    Alchemy model.

    For details on the fields see section 14.6.1 of `Open Energy Metadata
    <https://github.com/OpenEnergyPlatform/ oemetadata/blob/develop/metadata/
    v141/metadata_key_description.md>`_ standard.
    The fields `name` and `type` are automatically filled, the `description`
    and `unit` must be filled manually.

    Parameters
    ----------
    model : sqlalchemy.ext.declarative.declarative_base()
        SQLA model

    Returns
    -------
    list of dict
        Resource fields
    """
    fields = []

    # adjust to match the types of your columns
    sqlalchemy_type_map = {
      "BIGINT": "integer",
      "VARCHAR": "string"
    }

    for col in model.__table__.columns:
      print("\"" + str(col).split(".")[1] + ":\" ")
      field = {
        "name": col.name,
        "description": "",
        "type": sqlalchemy_type_map.get(str(col.type), col.type),
        "unit": None
      }
      fields.append(field)

    return fields


def generate_resource_fields_from_db_table(schema, table):
    """ Generate a template for the resource fields for metadata from a
    database table.

    For details on the fields see section 14.6.1 of `Open Energy Metadata
    <https://github.com/OpenEnergyPlatform/ oemetadata/blob/develop/metadata/
    v141/metadata_key_description.md>`_ standard.
    The fields `name` and `type` are automatically filled, the `description`
    and `unit` must be filled manually.

    Parameters
    ----------
    schema : str
        The target table's database schema
    table : str
        Database table on which to put the given comment

    Returns
    -------
    list of dict
        Resource fields
    """
    ischema_names['geom'] = Geometry

    table = Table(table,
                  MetaData(),
                  schema=schema,
                  autoload=True,
                  autoload_with=engine())

    return [{'name': col.name,
             'description': '',
             'type': str(col.type).lower(),
             'unit': None}
            for col in table.c]
