from pymongo import MongoClient, ReturnDocument
from bson.objectid import ObjectId
from bson.errors import InvalidId


access_url = 'case-executor-datastore:27017'


def catch_invalid_id(form_operator):
    def catch_wrapper(*args):
        try:
            return form_operator(*args)
        except InvalidId:
            raise ValueError('{} is not a valid ID. '.format(args[1]))
    return catch_wrapper


class CaseStatus:
    WAITING = 'WAITING'
    EXECUTING = 'EXECUTING'
    FINISHED = 'FINISHED'
    SUSPENDED = 'SUSPENDED'
    ERROR = 'ERROR'
    WAITING_SUSPENDED = 'WAITING_SUSPENDED'


class CaseCollection:

    def __init__(self, client):
        self.client = client
        self.db = self.client.cidev_db
        self.case_collection = self.db.case_collection

    @catch_invalid_id
    def get_case(self, cid):
        case = self.case_collection.find_one(ObjectId(cid))
        if not case:
            raise ValueError(f'Case with id {cid} not found.')
        case['_id'] = str(case['_id'])
        return case

    @catch_invalid_id
    def get_if_waiting(self, cid):
        """
        Fetches a case if status is WAITING, and sets status to EXECUTING
        :param cid: The ID of the case.
        :return: The case object, or None if it does not exist or is already being executed.
        """
        case = self.case_collection.find_one_and_update({'_id': ObjectId(cid), 'status': CaseStatus.WAITING},
                                                        {'$set': {'status': CaseStatus.EXECUTING}},
                                                        return_document=ReturnDocument.AFTER)
        if not case:
            return None
        case['_id'] = str(case['_id'])
        return case

    def get_first_waiting(self):
        """
        Fetches the first case where status is WAITING, and sets status to EXECUTING
        :return: The case object, or None if it does not exist or is already being executed.
        """
        case = self.case_collection.find_one_and_update({'status': CaseStatus.WAITING},
                                                        {'$set': {'status': CaseStatus.EXECUTING}},
                                                        return_document=ReturnDocument.AFTER)
        if not case:
            return None
        case['_id'] = str(case['_id'])
        return case

    def get_first_waiting_suspended(self):
        """
        Fetches the first case where status is WAITING_SUSPENDED, and sets status to EXECUTING
        :return: The case object, or None if it does not exist or is already being executed.
        """
        case = self.case_collection.find_one_and_update({'status': CaseStatus.WAITING_SUSPENDED},
                                                        {'$set': {'status': CaseStatus.EXECUTING}},
                                                        return_document=ReturnDocument.AFTER)
        if not case:
            return None
        case['_id'] = str(case['_id'])
        return case

    def get_all_cases(self):
        with self.case_collection.find({}) as cases:
            result = []
            for case in cases:
                case['_id'] = str(case['_id'])
                result.append(case)
        return result

    def add_case(self, case):
        return str(self.case_collection.insert_one(case).inserted_id)

    @catch_invalid_id
    def update_case(self, cid, updates):
        updates = dict(updates)
        try:
            # Delete '_id' if it exists, as it is immutable
            del updates['_id']
        except KeyError:
            pass
        updates = {'$set': updates}
        
        update_res = self.case_collection.update_one({'_id': ObjectId(cid)}, updates)

        if update_res.matched_count == 0:
            raise ValueError('Case with ID {} does not exist.'.format(cid))

    @catch_invalid_id
    def delete_case(self, cid):
        del_res = self.case_collection.delete_one({'_id': ObjectId(cid)})
        if del_res.deleted_count == 0:
            raise ValueError('Case with ID {} does not exist.'.format(cid))

    
    def delete_all(self):
        self.case_collection.delete_many()
