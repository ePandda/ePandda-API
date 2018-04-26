from flask_restful import Resource, Api
from flask import current_app, url_for
from base import baseResource

#
# Emit API banner
#
class banner(baseResource):
    def process(self):

        routes = []
        for rule in current_app.url_map.iter_rules():
          print rule
          if (rule.endpoint == 'static'):
              continue

          options = {}
          for arg in rule.arguments:
            options[arg] = "[{0}]".format(arg)

          url = url_for(rule.endpoint, **options)
          print rule.endpoint
          print rule.methods
          endpoint = self.loadEndpoint(rule.endpoint)
          print endpoint
          if endpoint is None:
              continue

          desc = endpoint.description();
          if desc['private'] is True:
              continue
          routes.append({'url': url, 'methods': ",".join(rule.methods), 'name': desc['name'], 'description': desc['description'] })

        return self.respond({
          'description': 'ePANDDA REST API guide',
          'routes': routes
        }, "routes")

    def description(self):
        return {
            'name': 'API Info',
            'maintainer': 'Seth Kaufman',
            'maintainer_email': 'seth@epandda.org',
            'description': 'Summary of available endpoints',
            'private': False,
            'params': []
        }
