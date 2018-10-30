from case_collection import CaseCollection
import requests

case_collection = CaseCollection()

block_url = 'workflow-block-service:8080/'


def post_json(endpoint, body):
    url = block_url + endpoint
    response = requests.post(url, json=body)
    return response.json()


def add_case(workflow, input_data):
    case = {
        "workflow": workflow,
        "data": input_data,
        "step": 0,
    }

    case_collection.add_case(case)


def execute_case(cid):
    case = case_collection.get_if_not_executing(cid)
    if not case:
        return

    workflow = case['workflow']
    step = case['step']

    while True:
        step_item = workflow['blocks'][step]

        if step_item.type == "action":
            params = step_item['params']
            # TODO Insert data from data store and previous step
            result = post_json(step_item['name'], {'params': params})

            if result['type'] == 'result':
                case['previous_outputs'] = result['data']

                for saved_output, save_to in step_item['save_outputs'].items():
                    if saved_output in result['data']:
                        # TODO Split on '.' to generate deeper structure?
                        case['store'][save_to] = result['data'][saved_output]
                step = step_item['next_block']
            elif result['type'] == 'suspend':
                case['suspended'] = True
                # TODO Need to save state, and handle the reason for suspension
                pass

        # Save the new state of the case (ensures that we can continue after a crash)
        case_collection.update_case(cid, case)
