from pymongo import MongoClient
from bson.objectid import ObjectId

access_url = 'case-executor-datastore:27017'


class CaseStatus:
    WAITING = 'WAITING'
    EXECUTING = 'EXECUTING'
    FINISHED = 'FINISHED'
    SUSPENDED = 'SUSPENDED'
    ERROR = 'ERROR'


class CaseCollection:

    def __init__(self):
        self.client = MongoClient(access_url)
        self.db = self.client.cidev_db
        self.case_collection = self.db.case_collection

    def get_case(self, cid):
        case = self.case_collection.find_one(ObjectId(cid))
        if not case:
            return None
        case['_id'] = str(case['_id'])
        return case

    def get_if_waiting(self, cid):
        """
        Fetches a case if status is WAITING, and sets status to EXECUTING
        :param cid: The ID of the case.
        :return: The case object, or None if it does not exist or is already being executed.
        """
        case = self.case_collection.find_one_and_update({'_id': ObjectId(cid), 'status': CaseStatus.WAITING},
                                                        {'$set': {'status': CaseStatus.EXECUTING}})
        if not case:
            return None
        case['_id'] = str(case['_id'])
        return case

    def get_first_waiting(self):
        """
        Fetches the first case where status is WAITING, and sets status to EXECUTING
        :param cid: The ID of the case.
        :return: The case object, or None if it does not exist or is already being executed.
        """
        case = self.case_collection.find_one_and_update({'status': CaseStatus.WAITING},
                                                        {'$set': {'status': CaseStatus.EXECUTING}})
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
    
    def update_case(self, cid, updates):
        updates = dict(updates)
        try:
            # Delete '_id' if it exists, as it is immutable
            del updates['_id']
        except KeyError:
            pass
        updates = {'$set': updates}
        
        self.case_collection.update_one({'_id': ObjectId(cid)}, updates)
    
    def delete_case(self, cid):
        self.case_collection.delete_one({'_id': ObjectId(cid)})
    
    def delete_all(self):
        self.case_collection.delete_many()
