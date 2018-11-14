from http import HTTPStatus
from pymongo import MongoClient
from flask import Flask, request, jsonify
import requests

from case_collection import CaseCollection, CaseStatus, access_url


app = Flask(__name__)
case_collection = CaseCollection(MongoClient(access_url))

workflow_editor_service_url = 'http://workflow-editor-service:8080/'


def add_case(workflow, input_data):
    """
    Adds a new case with the given workflow and input data.
    :param workflow: The workflow that the case is an instance of.
    :param input_data: The input data to the workflow.
    :return: The ID of the newly created case.
    """
    case = {
        "workflow": workflow,
        "store": {"input": input_data},
        "previous_outputs": input_data,
        "step": workflow['start_block'],
        "status": CaseStatus.WAITING
    }

    return case_collection.add_case(case)


@app.route('/', methods=['GET'])
def get_all_cases():
    cases = case_collection.get_all_cases()
    return jsonify({
        'data': cases
    })


@app.route('/<cid>', methods=['GET'])
def get_single_case(cid):
    try:
        case = case_collection.get_case(cid)
        return jsonify({
            'data': case
        })
    except ValueError as e:
        return str(e), HTTPStatus.NOT_FOUND


@app.route('/<cid>/store', methods=['GET'])
def get_case_store(cid):
    try:
        case = case_collection.get_case(cid)
        return jsonify({
            'data': case['store']
        })
    except ValueError as e:
        return str(e), HTTPStatus.NOT_FOUND


@app.route('/<cid>/resume/', methods=['GET'], strict_slashes=False)
def resume_case(cid):
    try:
        case = case_collection.get_case(cid)
        if case['status'] != CaseStatus.SUSPENDED:
            return 'Case is not suspended!', HTTPStatus.BAD_REQUEST
        case['status'] = CaseStatus.WAITING_SUSPENDED
        case_collection.update_case(cid, case)
        return '', HTTPStatus.OK
    except ValueError as e:
        return str(e), HTTPStatus.NOT_FOUND


@app.route('/execute_workflow/<wid>', methods=['POST'])
def execute_workflow(wid):
    workflow = requests.get('{url_base}{wid}'.format(url_base=workflow_editor_service_url, wid=wid))
    if workflow.status_code == HTTPStatus.NOT_FOUND:
        return f'Workflow with id {wid} not found', HTTPStatus.NOT_FOUND
    workflow = workflow.json()['data']
    form_data = request.get_json()
    cid = add_case(workflow, form_data)
    return cid, HTTPStatus.CREATED


# Only for testing purposes - should use WSGI server in production
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
