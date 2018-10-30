from flask import Flask, request
from http import HTTPStatus
import executor
import requests

app = Flask(__name__)


@app.route('/')
def hello_docker():
    return 'Hello Docker!'


@app.route('/execute_workflow/<wid>', methods=['POST'])
def execute_workflow(wid):
    workflow = requests.get('http://workflow-editor-service:8080/{wid}'.format(wid=wid)).json()
    form_data = request.get_json()
    executor.add_case(workflow, form_data)
    return '', HTTPStatus.CREATED


# Only for testing purposes - should use WSGI server in production
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)