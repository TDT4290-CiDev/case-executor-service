from flask import Flask, request, jsonify
from http import HTTPStatus
import executor
import requests
from case_collection import CaseCollection

from threading import Thread

app = Flask(__name__)

case_collection = CaseCollection()


@app.route('/')
def get_all_cases():
    cases = case_collection.get_all_cases()
    return jsonify({
        'data': cases
    })


@app.route('/<cid>')
def get_single_case(cid):
    case = case_collection.get_case(cid)
    return jsonify({
        'data': case
    })


@app.route('/<cid>/store')
def get_case_store(cid):
    case = case_collection.get_case(cid)
    return jsonify({
        'data': case['store']
    })


@app.route('/execute_workflow/<wid>', methods=['POST'])
def execute_workflow(wid):
    workflow = requests.get('http://workflow-editor-service:8080/{wid}'.format(wid=wid)).json()['data']
    form_data = request.get_json()
    cid = executor.add_case(workflow, form_data)
    thread = Thread(target=executor.execute_case(cid))
    thread.start()
    return cid, HTTPStatus.CREATED


# Only for testing purposes - should use WSGI server in production
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
