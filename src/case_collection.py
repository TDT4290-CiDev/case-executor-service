from pymongo import MongoClient
from bson.objectid import ObjectId

access_url = 'case-executor-datastore:27017'


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

    def get_all_cases(self):
        with self.case_collection.find({}) as cases:
            result = []
            for case in cases:
                case['_id'] = str(case['_id'])
                result.append(case)
        return result

    def add_case(self, case):
        return self.case_collection.insert_one(case).inserted_id
    
    def update_case(self, cid, updates):
        updates = {'$set': updates}
        
        self.case_collection.update_one({'_id': ObjectId(cid)}, updates)
    
    def delete_case(self, cid):
        self.case_collection.delete_one({'_id': ObjectId(cid)})
    
    def delete_all(self):
        self.case_collection.delete_many()
