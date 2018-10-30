from case_collection import CaseCollection
from dotmap import DotMap
from http import HTTPStatus
import requests
from case_collection import CaseStatus

case_collection = CaseCollection()


block_url = 'http://workflow-block-service:8080/'

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
    url = block_url + endpoint
    response = requests.post(url, json=body)
    if response.status_code == HTTPStatus.OK:
        return response.json()
    else:
        raise Exception('Endpoint {} returned status {}: {}'.format(endpoint, response.status_code, response.text))


def add_case(workflow, input_data):
    case = {
        "workflow": workflow,
        "store": {"input": input_data},
        "previous_outputs": input_data,
        "step": workflow['start_block'],
        "status": CaseStatus.WAITING
    }

    return case_collection.add_case(case)


def case_error(case, error):
    case['status'] = CaseStatus.ERROR
    case['error'] = error
    case_collection.update_case(case['_id'], case)


def execute_case(cid):
    case = case_collection.get_if_waiting(cid)
    if not case:
        return

    try:
        workflow = case['workflow']
        step = case['step']

        while True:
            step_item = workflow['blocks'][step]

            if step_item['type'] == 'action':
                block_info = requests.get(block_url + step_item['name'] + '/info').json()

                params = step_item['params']
                insert_params = DotMap({'store': case['store'],
                                        'outputs': case['previous_outputs']})
                for p_name, p_value in params.items():
                    if type(p_value) == str:
                        params[p_name] = p_value.format_map(insert_params)

                cleaned_params = {}
                for p in block_info['params']:
                    try:
                        typ = type_map[block_info['params'][p]['type']]
                        cleaned_params[p] = typ(params[p])
                    except KeyError:
                        case_error(case, 'Case is missing parameter ' + p + ' required by the block ' + step_item['name'] +
                                   'in step ' + step)
                        break
                    except ValueError:
                        case_error(case, 'Failed to cast parameter value for parameter ' + p + ' in step ' + step)
                        break

                try:
                    result = post_json(step_item['name'], {'params': cleaned_params})
                except Exception as e:
                    case_error(case, str(e))
                    break

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
            case_collection.update_case(cid, case)

            # If the next step is '-1', we have reached a terminal state
            if step == '-1':
                break
    except Exception as e:
        case_error(case, 'An unexpected error occured: ' + str(e))
