#
# This generates a static endpoint documentation page for the ePANDDA website
# It is refreshed daily/weekly so it should be up to date as it pulls from the endpoints themselves
#

import requests
import json

banner = requests.get('https://api.epandda.org')

doc = open('../../../sites/epandda/endpoint_doc.html', 'w')

endpoint_doc = '''
<section id="documentation-endpoint" class="container-full">
<div class="row">
	<div id="endpoint-header" class="col-12">
		<h2>ENDPOINTS</h2>
	</div>
</div>
'''

if banner.status_code == 200:
	data = banner.json()

	end_sections = []

	for endpoint in data['routes']:
		if endpoint in ['/']:
			continue
		route = 'https://api.epandda.org' + endpoint['url']
		description = requests.get(route)

		if description.status_code == 200:
			if len(endpoint['url'][1:]) < 1:
				continue
			params = description.json()
			if not params:
				continue
			desc = params['description'] if 'description' in params else 'This is a temporary description'
			point_row = '''
<div class="row">
	<div class="col-12">
		<h2>{0}</h2>
		<p>Endpoint: <a href="api.epandda.org/{2}" target="_blank">api.epandda.org/{2}</a><br/>
		{1}</p>
			'''.format(params['name'], desc, endpoint['url'][1:])

			param_table_open = '''
		<table class="parameterTable">
			<tr>
				<th>Name</th>
				<th>Label</th>
				<th>Type</th>
				<th>Required?</th>
				<th>Description</th>
			</tr>
			'''

			if 'params' in params:
				rows = []

				for param in params['params']:
					print param
					param_name = param['name'] if 'name' in param else ''
					param_type = param['type'] if 'type' in param else ''
					param_label = param['label'] if 'label' in param else ''
					param_req = param['required'] if 'required' in param else ''
					param_desc = param['description'] if 'description' in param else ''
					param_row = '''
			<tr>
				<td>{0}</td>
				<td>{1}</td>
				<td>{2}</td>
				<td>{3}</td>
				<td>{4}</td>
			</tr>
					'''.format(param_name, param_label, param_type, param_req, param_desc)
					rows.append(param_row)

				table_rows = ' '.join(rows)

			param_table_close = '''
		</table>
	</div>
</div>
			'''

			endpoint_section = ' '.join([point_row, param_table_open, table_rows, param_table_close])

			end_sections.append(endpoint_section)
		else:
			print "ERROR " + str(description.status_code)

	point_html = ' '.join(end_sections)

doc_end = '''
</section>
'''
full_html = ' '.join([endpoint_doc, point_html, doc_end])
doc.write(full_html)
