from models.base.dataset import Dataset as BaseDataset
import json
import datetime
from task_utils.upload_file import upload_file
import os

class Dataset(BaseDataset):
    schemaPath = "schemas/dataset.schema.json"
    schema = None

    def __init__(self, metadata_dict=False, from_file=False):
        if from_file:
            meta_data = open(from_file, 'r')
            meta_data = json.load(meta_data)
            self.__storage_location = {
                "bucket": meta_data.get("__bucket", None),
                "key": meta_data.get("__key", None)
            }
            super().__init__(meta_data)
        elif metadata_dict:
            super().__init__(metadata_dict)
            self.__storage_location = {
                "bucket": metadata_dict.get("__bucket", None),
                "key": metadata_dict.get("__key", None)
            }
        else:
            super().__init__()
            print("No metadata provided")

        self.localFilePath = None
        self.destinationMetadataCollection = None
        self.schema = open(self.schemaPath, 'r')
        self.schema = json.load(self.schema)

        if self.date is None:
            self.setDateToNow()
        
        self.setCoverageDate(self.date)

    def setLocalFilePath(self, path):
        self.localFilePath = path

        # Test if file exists, if exists, store file file size
        try:
            self.file_size = round(os.path.getsize(path) / (1024 * 1024), 3)
        except:
            print("File not found")
            self.file_size = None

    def getLocalFilePath(self):
        return self.localFilePath

    def setDestinationMetadataCollection(self, collection):
        self.destinationMetadataCollection = collection

    def storeDatasetMetadata(self, mongodb):
        update_timestamps = {
            "last_update": datetime.datetime.now()
        }
        mongo_collection = "datasets"
        doc_query = {
            "identifier": self.identifier
        }
        doc_attrs = update_timestamps
        for prop in self.schema['properties']:
            if prop in self.__dict__ and self.__dict__[prop] is not None:
                doc_attrs[prop] = self.__dict__[prop]
        
        extra_props = ["__bucket", "__key", "file_size"]
        for prop in extra_props:
            if prop in self.__dict__ and self.__dict__[prop] is not None:
                doc_attrs[prop] = self.__dict__[prop]

        doc_attrs["#schema"] = self.schema['$id']
        new_values = {
            "$set": doc_attrs
        }
        mongodb[mongo_collection].update_one(doc_query, new_values, upsert=True)

    def uploadDatasetFile(self, storage_params):
        upload_file(
            file_path=self.localFilePath,
            s3_client=storage_params.get("s3_client", None),
            s3_resource=storage_params.get("s3_resource", None),
            bucket=self.__storage_location["bucket"],
            object_name=self.__storage_location["key"],
            make_public=True
        )

    def setDateToNow(self):
        self.date = datetime.datetime.now()

    def setCoverageDate(self, date):
        if date is None:
            return
        if self.coverage is None:
            return
        date_formatted = date.strftime("%Y-%m-%d")
        self.coverage = self.coverage.replace("{{DATE}}", date_formatted)

    def printProperties(self):
        print("-- Dataset properties --")
        allPropsDefined = True
        missingPropsCount = 0
        for prop in self.schema['properties']:
            if prop in self.__dict__:
                
                if self.__dict__[prop] is None:
                    print("❌ -",prop, ":" ,self.__dict__[prop])
                    allPropsDefined = False
                    missingPropsCount += 1
                else:
                    print("✅ -",prop, ":" ,self.__dict__[prop])
            else:
                print(prop, "not defined")
                allPropsDefined = False
                missingPropsCount += 1
        
        if allPropsDefined:
            print("✅ All properties defined")
        else:
            print(" Missing properties: ", missingPropsCount, " ❌")
        print("-- End Dataset properties --")
        pass

    def printMissingProperties(self):
        for prop in self.schema['properties']:
            if prop not in self.__dict__:
                print(prop, "not defined")
            elif self.__dict__[prop] is None:
                print(f"{prop} is None")
                
        pass