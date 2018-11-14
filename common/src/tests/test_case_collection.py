import unittest
import mongomock
from bson.objectid import ObjectId

from case_collection import CaseCollection


class CollectionTest(unittest.TestCase):
    mock_coll = None
    initial_cases = None

    def setUp(self):
        self.mock_coll = CaseCollection(mongomock.MongoClient())
        self.initial_cases = [dict(title='case1'), dict(title='case2')]
        for f in self.initial_cases:
            f['_id'] = str(self.mock_coll.db.case_collection.insert_one(f).inserted_id)

    def test_add_return_valid_id(self):
        add_response = self.mock_coll.add_case({"title": "test"})
        self.assertTrue(ObjectId.is_valid(add_response))

    def test_read_one(self):
        _id = self.initial_cases[0]['_id']
        read_response = self.mock_coll.get_case(_id)
        self.assertEqual(read_response, self.initial_cases[0])

    def test_read_all(self):
        all = self.mock_coll.get_all_cases()
        self.assertEqual(all, self.initial_cases)

    def test_update_no_return(self):
        _id = self.initial_cases[0]['_id']
        update_res = self.mock_coll.update_case(_id, dict(new_title='Updated case1'))
        self.assertIsNone(update_res)

    def test_delete_no_return(self):
        _id = self.initial_cases[0]['_id']
        deleteRes = self.mock_coll.delete_case(_id)
        self.assertIsNone(deleteRes)

    def test_invalid_id_format(self):
        inv_id = '0'
        with self.assertRaises(ValueError):
            self.mock_coll.get_case(inv_id)
        with self.assertRaises(ValueError):
            self.mock_coll.update_case(inv_id, {})
        with self.assertRaises(ValueError):
            self.mock_coll.delete_case(inv_id)

    def test_valid_but_nonexisting_id(self):
        # Creating an id of correct length, but with all 5s.
        inv_id = '5'*len(self.initial_cases[0]['_id'])
        with self.assertRaises(ValueError):
            self.mock_coll.get_case(inv_id)
        with self.assertRaises(ValueError):
            self.mock_coll.update_case(inv_id, {})
        with self.assertRaises(ValueError):
            self.mock_coll.delete_case(inv_id)