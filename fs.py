import os
from datetime import datetime, timedelta

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

class Firestore():

    SERVER_TIMESTAMP = firestore.SERVER_TIMESTAMP
    DELETE_FIELD = firestore.DELETE_FIELD

    def __init__(self):
        self.curPath = os.path.dirname(os.path.abspath(__file__))
        keyFile = self.curPath + "/serviceAccountKey.json"
        
        if (not len(firebase_admin._apps)):
            self.cred = credentials.Certificate(keyFile)
            firebase_admin.initialize_app(self.cred)
        
        self.fs = firestore.client()
        self.ts = datetime.now().strftime('%y%m%d%H%M')

    @classmethod
    def collection(cls, collection):
       return cls().fs.collection(collection)

    @classmethod
    def insert(cls, collection, key, data):
        cls().fs.collection(collection).document(key).set(data)
    
    @classmethod
    def set(cls, collection, key, data):
        cls().fs.collection(collection).document(key).set(data, merge=True)

    @classmethod
    def set2(cls, collection, key, subcollection, data):
        cls().fs.collect(collection).document(key).collection(subcollection).set(data, merge=True)
    
    @classmethod
    def update(cls, collection, key, data):
        cls().fs.collection(collection).document(key).update(data)

    @classmethod
    def delete(cls, collection, id):
        cls().fs.collection(collection).document(id).delete()
        #docs = cls().fs.collection(collection).where('type', '==', q).stream()

        #for doc in docs:
            #print(f'{doc.id}')
            #cls().fs.collection(collection).document(f'{doc.id}').delete()
