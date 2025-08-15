import os
import typing
import datetime
from functools import reduce

import pandas as pd
import databento as db

class Client():

    def __init__(self,root="data/") -> None:
        self.client_ = db.Historical()
        self.root_ = root
    
    # List all databento feeds
    def get_feeds(self):

        pth = os.path.join(self.root_,"feeds.csv")
        if os.path.exists(pth):
            return pd.read_csv(pth)

        pub = pd.DataFrame(self.client_.metadata.list_publishers())
        pub["schemas"] = pub.dataset.apply(lambda dn:self.client_.metadata.list_schemas(dn))

        pub.to_csv(pth,index=False)

        return pub
    
    # Compose query dictionary
    def qdict(self,date:str,symbols:typing.List[str],dataset:str,schema:str,start:str="09:30",end:str="16:00",tz="America/New_York",stype_in:str="raw_symbol",limit=10000) -> typing.Dict:
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
        return os.path.join(self.root_,date,".".join([dataset,schema,"dbn"]))
    
    # Run one query
    def onequery(self,mode="plan",**kwargs) -> typing.Dict|db.DBNStore:
        dct = self.qdict(**kwargs)
        start = dct["start"]
        del(dct["start"])

        if mode=="plan":
            return {"num_records" : self.client_.metadata.get_record_count(start=start,**dct), "cost" : self.client_.metadata.get_cost(start=start,**dct)}
        elif mode=="run":
            pth = self.path(date=kwargs["date"],dataset=kwargs["dataset"],schema=kwargs["schema"])

            data = self.client_.timeseries.get_range(start=start,**dct)
            print(f"[+] Saving query to: {pth}")
            data.to_file(pth)

            return data
        else:
            raise ValueError("Mode not recognized")
    
    # Display cost and number of records for a set of queries
    def plan(self,queries:pd.DataFrame) -> pd.DataFrame:
        qplan = queries.copy()
        qplan[["num_records","cost"]] = qplan.apply(lambda r:pd.Series(self.onequery(mode="plan",**r)),axis=1)
        return qplan
    
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
        dirs = [ os.path.join(self.root_,x) for x in os.listdir(self.root_) if x.startswith("20") ]
        files = [ [os.path.join(d,f) for f in os.listdir(d)] for d in dirs ]

        df = pd.DataFrame({"filename": reduce(sum,files)})
        df[["date","dataset","schema"]] = df.filename.apply(self._splitpath)

        df.date = pd.DatetimeIndex(df.date)

        return df

    ###################################################################

    # List all options for one underlier
    def fetch_options(self,date:str,ticker:str,mode="plan") -> typing.Dict|db.DBNStore:
        return self.onequery(mode=mode,date=date,dataset="OPRA.PILLAR",symbols=f"{ticker}.OPT",stype_in="parent",schema="definition",start="00:00",tz="UTC",limit=None)
    
    def read_options(self,date:str) -> pd.DataFrame:
        pth = self.path(date,"OPRA.PILLAR","definition")
        return db.DBNStore.from_file(pth).to_df()
    
    def filter_options(self,date:str,clause:typing.Callable) -> typing.List[str]:
        opts = self.read_options(date)
        return list(opts[clause(opts)]["raw_symbol"])