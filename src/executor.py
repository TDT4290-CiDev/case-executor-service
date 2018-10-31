from case_collection import CaseCollection
from dotmap import DotMap
from http import HTTPStatus
import requests
from multiprocessing import Process
from case_collection import CaseStatus
import time

case_collection = CaseCollection()


block_url = 'http://workflow-block-service:8080/'
NUM_WORKERS = 5
workers = []

type_map = {
    'string': str,
    'str': str,
    'float': float,
    'double': float,
    'integer': int,
    'int': int,
    'boolean': bool,
    'bool': bool
}


def post_json(endpoint, body):
    """
    Sends a post request to the given endpoint, and returns result as a JSON object.
    :param endpoint: The endpoint that should be called on the WorkflowBlockService.
    :param body: The body of the request as a JSON object.
    :return: The response from the block.
    """
    url = block_url + endpoint
    response = requests.post(url, json=body)
    if response.status_code == HTTPStatus.OK:
        return response.json()
    else:
        raise Exception('Endpoint {} returned status {}: {}'.format(endpoint, response.status_code, response.text))


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


def case_error(case, error):
    """
    Set error fields for the given case, and persist it to the database.
    :param case: The case that has given an error.
    :param error: The error message.
    """
    case['status'] = CaseStatus.ERROR
    case['error'] = error
    case_collection.update_case(case['_id'], case)


def execute_block(case, block, step):
    """
    Executes a block by compiling parameters and calling the correct endpoint on WorkflowBlockService.
    :param case: The case that is being executed.
    :param block: The block that should be executed.
    :param step: The step we are currently on in the case.
    :return: Results from the block.
    """
    block_info = requests.get(block_url + block['name'] + '/info').json()

    params = block['params']
    insert_params = DotMap({'store': case['store'],
                            'outputs': case['previous_outputs']})
    for p_name, p_value in params.items():
        if type(p_value) == str:
            # Insert parameters from store and previous outputs.
            params[p_name] = p_value.format_map(insert_params)

    cleaned_params = {}
    for p in block_info['params']:
        try:
            # Try to cast parameter to correct type.
            # Required since all data from store or previous step are inserted as strings.
            typ = type_map[block_info['params'][p]['type']]
            cleaned_params[p] = typ(params[p])
        except KeyError:
            case_error(case, 'Case is missing parameter ' + p + ' required by the block ' +
                       block['name'] + 'in step ' + step)
            return None
        except ValueError:
            case_error(case, 'Failed to cast parameter value for parameter ' + p + ' in step ' + step)
            return None

    result = post_json(block['name'], {'params': cleaned_params})
    return result


def execute_case(case):
    """
    Handles the execution of a case, including the execution of single blocks and branching.
    :param cid: The case ID.
    """
    try:
        workflow = case['workflow']
        step = case['step']

        while True:
            step_item = workflow['blocks'][step]

            if step_item['type'] == 'action':
                result = execute_block(case, step_item, step)

                if not result:
                    return

                if result['type'] == 'result':
                    case['previous_outputs'] = result['data']

                    store = DotMap(case['store'])
                    for saved_output, save_to in step_item['save_outputs'].items():
                        if saved_output in result['data']:
                            path = save_to.split('.')
                            item = store
                            for i in range(len(path) - 1):
                                item = item[path[i]]
                            item[path[-1]] = result['data'][saved_output]
                    step = step_item['next_block']
                    case['store'] = store.toDict()
                elif result['type'] == 'suspend':
                    case['suspended'] = True
                    # TODO Need to save state, and handle the reason for suspension
                    pass

            if step == '-1':
                case['status'] = CaseStatus.FINISHED

            # Save the new state of the case (ensures that we can continue after a crash)
            case_collection.update_case(case['_id'], case)

            # If the next step is '-1', we have reached a terminal state
            if step == '-1':
                break
    except Exception as e:
        case_error(case, 'An unexpected error occured: ' + str(e))


def worker_target():
    while True:
        case = case_collection.get_first_waiting()
        if not case:
            time.sleep(5)
        else:
            execute_case(case)


def start_workers():
    for i in range(NUM_WORKERS):
        worker = Process(target=worker_target)
        workers.append(worker)
        worker.start()
