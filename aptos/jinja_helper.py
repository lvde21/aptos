from jinja2 import Environment, FileSystemLoader

class JinjaHelper:

    jinja_env = None

    @classmethod
    def create_jinja_env(cls):
        file_loader = FileSystemLoader('templates')
        JinjaHelper.jinja_env = Environment(loader=file_loader)

    @staticmethod
    def get_template(template_name):
        if JinjaHelper.jinja_env is None:
            JinjaHelper.create_jinja_env()
        return JinjaHelper.jinja_env.get_template(template_name)