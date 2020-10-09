from ..primitive import Array, Object, Reference, Enumeration
from ..jinja_helper import JinjaHelper


class AvroSchemaVisitor:

    def visit_empty_schema(self, schema, *args):  # pragma: no cover
        return

    def visit_enumeration(self, enumeration, *args):
        return {
            'type': 'enum', 'name': enumeration.title,
            'symbols': enumeration.enum}

    def visit_boolean(self, boolean, *args):
        return {'type': 'boolean'}

    def visit_null(self, null, *args):
        return {'type': 'null'}

    def visit_number(self, number, *args):
        return {'type': 'double'}

    def visit_integer(self, integer, *args):
        return {'type': 'long'}

    def visit_string(self, string, *args):
        return {'type': 'string'}

    def visit_array(self, array, *args):
        items = array.items.accept(self, *args)
        if 'fields' not in items:
            items = items['type']
        return {'type': 'array', 'items': items}

    def visit_array_list(self, array_list, *args):
        # TODO: should ArrayList types return unions?
        return {'type': [element.accept(self, *args).get('type') for element in array_list][0]}  # noqa: E501

    def visit_object(self, obj, *args):
        fields = []
        for name, member in obj.properties.items():
            field = member.accept(self, *args)
            if isinstance(member, (Array, Object, Reference, Enumeration)):
                field = {'type': field}
            field.update({'name': name, 'doc': member.description})
            fields.append(field)
        for element in obj.allOf:
            fields.extend(element.accept(self, *args).get('fields', [element.accept(self, *args)]))  # noqa: E501
        return {'type': 'record', 'name': obj.title, 'fields': fields}

    def visit_reference(self, reference, *args):
        if reference.resolved:
            return reference.value.accept(self, *args)

    def visit_union(self, union, *args):
        return {'type': union.type}

##-------------------------------------##

class ParquetSchemaVisitor:

    def determine_repetition(self, name, required_fields):
        if name in required_fields:
            return 'required'
        else:
            return 'optional'

    def visit_string(self, string, *args):
        return '{repetition} binary {name} (UTF8);\n'
    
    def visit_integer(self, integer, *args):
        return '{repetition} int32 {name};\n'

    def visit_object(self, obj, *args):
        fields = ''
        for name, member in obj.properties.items():
            placeholder_field = member.accept(self, *args)
            if isinstance(member, (Array, Object, Reference, Enumeration)):
                fields += placeholder_field
                fields += '\n'
            else:
                field = placeholder_field.format(repetition=self.determine_repetition(name, obj.required), name=name)
                fields += field
        return JinjaHelper.get_template('object.jinja2').render(obj_name=obj.title, body=fields)