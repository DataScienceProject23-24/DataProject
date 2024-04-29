class Handler (object):
    def __init__(self, dbPathOrUrl):
        self.dbPathOrUrl = dbPathOrUrl
    def getdbPathOrUrl(self):
        return self.dbPathOrUrl
    def setdbPathOrUrl(self,pathOrUrl):
        result = True
        if pathOrUrl not in self.dbPathOrUrl:
            self.dbPathOrUrl.add(pathOrUrl)
        else:
            result = False
        return result

#need revision    
class UploadHandler(Handler):
    def __init__(self,dbPathOrUrl):
        super().__init__(dbPathOrUrl)
    def pushDataToDb(self,path):
        pass

class MetadataUploadHandler(UploadHandler):
    pass

class ProcessDataUploadHandler(UploadHandler):
    def __init__(self,dbPathOrUrl):
        super().__init__(dbPathOrUrl)
        import pandas as pd
        from sqlite3 import connect
        process_json = pd.read_json("../resources/process.json") #need to change to path declaration
        
        object_ids=process_json[["object id"]]
        object_internal_id = []
        for idx, row in object_ids.iterrows():
            object_internal_id.append("object-" + str(idx+1))
        object_ids.insert(0, "objectId", pd.Series(object_internal_id, dtype="string"))
        
        norm_df=[]
        cols = ["acquisition","processing","modelling","optimising","exporting"]
        for col in cols:
            norm = pd.json_normalize(process_json[col])
            norm["tool"] = norm["tool"].astype("str")
            norm.insert((norm.shape[1]),"objectId",object_ids["objectId"])
            norm_df.append(norm)
        
        with connect(dbPathOrUrl) as con:
            norm_df[0].to_sql("Acquisition", con, if_exists="replace", index=False)
            norm_df[1].to_sql("Processing", con, if_exists="replace", index=False)
            norm_df[2].to_sql("Modelling", con, if_exists="replace", index=False)
            norm_df[3].to_sql("Optimizing", con, if_exists="replace", index=False)
            norm_df[4].to_sql("Exporting", con, if_exists="replace", index=False)

