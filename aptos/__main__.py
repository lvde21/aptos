import argparse
import json
import sys
import colorama

from termcolor import colored
from .parser import SchemaParser
from .primitive import Object
from .visitor import ValidationVisitor
from .schema.visitor import AvroSchemaVisitor, ParquetSchemaVisitor
from .jinja_helper import JinjaHelper


def validate(arguments):
    with open(arguments.schema) as fp:
        schema = json.load(fp)
    component = SchemaParser.parse(schema)
    instance = json.loads(arguments.instance)
    try:
        component.accept(ValidationVisitor(instance))
    except AssertionError as e:
        sys.exit(colored('error', 'red') + ' {!r}'.format(e.args[0]))
    print(colored('success', 'green') + ' instance {!r} is valid against the schema {!r}'.format(instance, arguments.schema))  # noqa: E501


def convert(arguments):
    with open(arguments.schema) as fp:
        schema = json.load(fp)
    component = SchemaParser.parse(schema)
    if not isinstance(component, Object):
        sys.exit(colored('error', 'red') + ' cannot convert schema {!r} into {!r} format, schema must be of type "object"'.format(arguments.schema, arguments.format))  # noqa: E501
    Visitor = {
        'avro': AvroSchemaVisitor,
        'parquet': ParquetSchemaVisitor
    }[arguments.format]
    converted_schema = component.accept(Visitor())
    print_schema(arguments.format, converted_schema)


def print_schema(schema_arg, converted_schema):
    if schema_arg == 'avro':
        print(json.dumps(converted_schema, indent=2))
    elif schema_arg == 'parquet':
        parquet_schema = JinjaHelper.get_template('inner_schema.jinja2')
        print(parquet_schema.render(body=converted_schema))


def main():
    # colorama works cross-platform to color text output in CLI
    colorama.init()

    parser = argparse.ArgumentParser(description='''
        aptos is a tool for validating client-submitted data using the
        JSON Schema vocabulary and converts JSON Schema documents into
        different data-interchange formats.
    ''', usage='%(prog)s [arguments] SCHEMA', epilog='''
        More information on JSON Schema: http://json-schema.org/''')
    subparsers = parser.add_subparsers(title='Arguments')

    validation = subparsers.add_parser(
        'validate', help='Validate a JSON instance')
    validation.add_argument(
        '-instance', type=str, default=json.dumps({}),
        help='JSON document being validated')
    validation.set_defaults(func=validate)

    conversion = subparsers.add_parser(
        'convert', help='''
        Convert a JSON Schema into a different data-interchange format''')
    conversion.add_argument(
        '-format', type=str, choices=['avro', 'parquet'], help='data-interchange format')
    conversion.set_defaults(func=convert)

    parser.add_argument(
        'schema', type=str, help='JSON document containing the description')

    arguments = parser.parse_args()
    arguments.func(arguments)


if __name__ == '__main__':
    main()
