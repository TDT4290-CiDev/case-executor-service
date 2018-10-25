from flask import Flask, request
from http import HTTPStatus
import executor

app = Flask(__name__)


@app.route('/')
def hello_docker():
    return 'Hello Docker!'


@app.route('/execute_workflow/<id>', methods=['POST'])
def execute_workflow(id):
    workflow = None  # TODO Fetch from workflow-editor-service
    form_data = request.get_json()
    executor.add_case(workflow, form_data)
    return HTTPStatus.CREATED


# Only for testing purposes - should use WSGI server in production
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)