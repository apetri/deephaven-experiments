import os
import typing
import itertools

import pandas as pd
import databento as db

class Client():

    def __init__(self,root="data/") -> None:
        self._client = db.Historical()
        self._root = root
        self._feeds = pd.DataFrame()

    @property
    def client(self):
        return self._client

    @property
    def feeds(self) -> pd.DataFrame:
        return self._feeds

    # List all databento feeds
    def get_feeds(self) -> None:

        pth = os.path.join(self._root,"feeds.csv")
        if os.path.exists(pth):

            pub = pd.read_csv(pth)
            pub["schemas"] = pub.schemas.fillna("").str.split("|")
            self._feeds = pub

            return None

        pub = pd.DataFrame(self._client.metadata.list_publishers())
        pub["schemas"] = pub.dataset.apply(lambda dn:self._client.metadata.list_schemas(dn))

        self._feeds = pub.copy()

        pub["schemas"] = pub.schemas.str.join("|")
        pub.to_csv(pth,index=False)

        return None

    # Compose query dictionary
    def qdict(self,date:str,symbols:typing.List[str],dataset:str,schema:str,start:str="09:30",end:str="16:00",tz="America/New_York",stype_in:str="raw_symbol",limit=None,**kwargs) -> typing.Dict:
        dct = { "dataset":dataset,
                "schema":schema, 
                "symbols":symbols,
                "stype_in":stype_in,
                "start":pd.Timestamp(f"{date}T{start}",tz=tz),
                "end":pd.Timestamp(f"{date}T{end}",tz=tz),
                "limit":limit
              }
        return dct
    
    #Path where query result will be saved
    def path(self,date:str,dataset:str,schema:str) -> str:
        return os.path.join(self._root,date.replace("-",""),".".join([dataset,schema,"dbn"]))

    # Cost of running one query
    def cost(self,**kwargs) -> typing.Dict:
        dct = self.qdict(**kwargs)
        start = dct["start"]
        del(dct["start"])

        return {"num_records" : self._client.metadata.get_record_count(start=start,**dct),
                "cost" : self._client.metadata.get_cost(start=start,**dct),
                "size_gb": self._client.metadata.get_billable_size(start=start,**dct) / 1024**3}

    # Run one query
    def onequery(self,mode="",**kwargs) -> typing.Dict|db.DBNStore:
        dct = self.qdict(**kwargs)
        start = dct["start"]
        del(dct["start"])

        match mode:

            case "run":
                pth = self.path(date=kwargs["date"],dataset=kwargs["dataset"],schema=kwargs["schema"])

                data = self._client.timeseries.get_range(start=start,**dct)
                print(f"[+] Saving query to: {pth}")
                data.to_file(pth)

                return data
            case "submit_batch":
                return self._client.batch.submit_job(start=start,**dct)
            case _:
                raise ValueError("Mode not recognized")
    
    # Fetch data and persist to disk
    def fetch(self,queries:pd.DataFrame) -> None:
        for r in queries.iterrows():
            self.onequery(mode="run",**r[1])
    
    # List data persisted to disk
    @staticmethod
    def _splitpath(pth) -> pd.Series:
        p1 = pth.split("/")
        p2 = p1[-1].split(".")

        return pd.Series({"date": p1[1], "dataset":".".join(p2[:2]),"schema":p2[-2]})

    def ls(self) -> pd.DataFrame:
        dirs = [ os.path.join(self._root,x) for x in os.listdir(self._root) if x.startswith("20") ]
        files = [ [os.path.join(d,f) for f in os.listdir(d) if f.endswith(".dbn")] for d in dirs ]

        df = pd.DataFrame({"filename": list(itertools.chain(*files))})
        df[["date","dataset","schema"]] = df.filename.apply(self._splitpath)

        df.date = pd.DatetimeIndex(df.date)

        return df

    def lsbatch(self) -> pd.DataFrame:
        r0 = os.path.join(self._root,"batch")
        dirs = [ os.path.join(r0,x) for x in os.listdir(r0) ]
        files = [ [os.path.join(d,f) for f in os.listdir(d) if f.endswith(".zst")] for d in dirs ]

        df = pd.DataFrame({"filename": list(itertools.chain(*files))})
        df["jobid"] = df.filename.apply(lambda x:x.split("/")[2])

        return df[["jobid","filename"]]

    ###################################################################

    # List all options for one underlier
    def fetch_options(self,date:str,ticker:str,mode="plan") -> typing.Dict|db.DBNStore:
        return self.onequery(mode=mode,date=date,dataset="OPRA.PILLAR",symbols=f"{ticker}.OPT",stype_in="parent",schema="definition",start="00:00",tz="UTC",limit=None)