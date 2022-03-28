import luigi
import wget
import gzip
import pandas as pd
from datetime import datetime
from luigi.contrib import gcs
from luigi.format import Nop

class GetNameBasics(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/name_basics.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/name.basics.tsv.gz", out="data/name_basics.tsv.gz")
    

class PushNameBasics(luigi.Task):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gzip_file = "name_basics.tsv.gz"
        self.tsv_file = "name_basics.tsv"
        self.parquet_file = "name_basics.parquet"

    def requires(self):
        return GetNameBasics()

    def output(self):
        return gcs.GCSTarget(f"gs://dtc_de_imdb/raw/{self.parquet_file}", format=Nop)

    def run(self):

        with gzip.open(f"data/{self.gzip_file}","rb") as filein:
            contents = filein.read()
        
        with open(f"data/{self.tsv_file}","wb") as fout:
            fout.write(contents)
        
        # Writing to Parquet file
        df = pd.read_csv(f"data/{self.tsv_file}",sep="\t", low_memory=False)
        df.to_parquet(f"data/{self.parquet_file}")

        with open(f"data/{self.parquet_file}","rb") as filein:
            contents = filein.read()
        
        with self.output().open("w") as fileout:
            fileout.write(contents)


class GetTitleAkas(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/title_akas.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/title.akas.tsv.gz", out="data/title_akas.tsv.gz")


class PushTitleAkas(luigi.Task):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gzip_file = "title_akas.tsv.gz"
        self.tsv_file = "title_akas.tsv"
        self.parquet_file = "title_akas.parquet"

    def requires(self):
        return GetTitleAkas()

    def output(self):
        return gcs.GCSTarget(f"gs://dtc_de_imdb/raw/{self.parquet_file}", format=Nop)

    def run(self):

        with gzip.open(f"data/{self.gzip_file}","rb") as filein:
            contents = filein.read()
        
        with open(f"data/{self.tsv_file}","wb") as fout:
            fout.write(contents)
        
        # Writing to Parquet file
        df = pd.read_csv(f"data/{self.tsv_file}",sep="\t", low_memory=False)
        df.to_parquet(f"data/{self.parquet_file}")

        with open(f"data/{self.parquet_file}","rb") as filein:
            contents = filein.read()
        
        with self.output().open("w") as fileout:
            fileout.write(contents)


class GetTitleBasics(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/title_basics.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/title.basics.tsv.gz", out="data/title_basics.tsv.gz")


class PushTitleBasics(luigi.Task):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gzip_file = "title_basics.tsv.gz"
        self.tsv_file = "title_basics.tsv"
        self.parquet_file = "title_basics.parquet"

    def requires(self):
        return GetTitleBasics()

    def output(self):
        return gcs.GCSTarget(f"gs://dtc_de_imdb/raw/{self.parquet_file}", format=Nop)

    def run(self):

        with gzip.open(f"data/{self.gzip_file}","rb") as filein:
            contents = filein.read()
        
        with open(f"data/{self.tsv_file}","wb") as fout:
            fout.write(contents)
        
        # Writing to Parquet file
        df = pd.read_csv(f"data/{self.tsv_file}",sep="\t", low_memory=False)
        df.to_parquet(f"data/{self.parquet_file}")

        with open(f"data/{self.parquet_file}","rb") as filein:
            contents = filein.read()
        
        with self.output().open("w") as fileout:
            fileout.write(contents)


class GetTitleCrew(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/title_crew.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/title.crew.tsv.gz", out="data/title_crew.tsv.gz")


class PushTitleCrew(luigi.Task):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gzip_file = "title_crew.tsv.gz"
        self.tsv_file = "title_crew.tsv"
        self.parquet_file = "title_crew.parquet"

    def requires(self):
        return GetTitleCrew()

    def output(self):
        return gcs.GCSTarget(f"gs://dtc_de_imdb/raw/{self.parquet_file}", format=Nop)

    def run(self):

        with gzip.open(f"data/{self.gzip_file}","rb") as filein:
            contents = filein.read()
        
        with open(f"data/{self.tsv_file}","wb") as fout:
            fout.write(contents)
        
        # Writing to Parquet file
        df = pd.read_csv(f"data/{self.tsv_file}",sep="\t", low_memory=False)
        df.to_parquet(f"data/{self.parquet_file}")

        with open(f"data/{self.parquet_file}","rb") as filein:
            contents = filein.read()
        
        with self.output().open("w") as fileout:
            fileout.write(contents)


class GetTitleEpisodes(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/title_episodes.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/title.episode.tsv.gz", out="data/title_episodes.tsv.gz")


class PushTitleEpisodes(luigi.Task):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gzip_file = "title_episodes.tsv.gz"
        self.tsv_file = "title_episodes.tsv"
        self.parquet_file = "title_episodes.parquet"

    def requires(self):
        return GetTitleEpisodes()

    def output(self):
        return gcs.GCSTarget(f"gs://dtc_de_imdb/raw/{self.parquet_file}", format=Nop)

    def run(self):

        with gzip.open(f"data/{self.gzip_file}","rb") as filein:
            contents = filein.read()
        
        with open(f"data/{self.tsv_file}","wb") as fout:
            fout.write(contents)
        
        # Writing to Parquet file
        df = pd.read_csv(f"data/{self.tsv_file}",sep="\t", low_memory=False)
        df.to_parquet(f"data/{self.parquet_file}")

        with open(f"data/{self.parquet_file}","rb") as filein:
            contents = filein.read()
        
        with self.output().open("w") as fileout:
            fileout.write(contents)


class GetTitlePrincipals(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/title_principals.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/title.principals.tsv.gz", out="data/title_principals.tsv.gz")


class PushTitlePrincipals(luigi.Task):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gzip_file = "title_principals.tsv.gz"
        self.tsv_file = "title_principals.tsv"
        self.parquet_file = "title_principals.parquet"

    def requires(self):
        return GetTitlePrincipals()

    def output(self):
        return gcs.GCSTarget(f"gs://dtc_de_imdb/raw/{self.parquet_file}", format=Nop)

    def run(self):

        with gzip.open(f"data/{self.gzip_file}","rb") as filein:
            contents = filein.read()
        
        with open(f"data/{self.tsv_file}","wb") as fout:
            fout.write(contents)
        
        # Writing to Parquet file
        df = pd.read_csv(f"data/{self.tsv_file}",sep="\t", low_memory=False)
        df.to_parquet(f"data/{self.parquet_file}")

        with open(f"data/{self.parquet_file}","rb") as filein:
            contents = filein.read()
        
        with self.output().open("w") as fileout:
            fileout.write(contents)


class GetTitleRatings(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/title_ratings.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/title.ratings.tsv.gz", out="data/title_ratings.tsv.gz")


class PushTitleRatings(luigi.Task):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gzip_file = "title_ratings.tsv.gz"
        self.tsv_file = "title_ratings.tsv"
        self.parquet_file = "title_ratings.parquet"

    def requires(self):
        return GetTitleRatings()

    def output(self):
        return gcs.GCSTarget(f"gs://dtc_de_imdb/raw/{self.parquet_file}", format=Nop)

    def run(self):

        with gzip.open(f"data/{self.gzip_file}","rb") as filein:
            contents = filein.read()
        
        with open(f"data/{self.tsv_file}","wb") as fout:
            fout.write(contents)
        
        # Writing to Parquet file
        df = pd.read_csv(f"data/{self.tsv_file}",sep="\t", low_memory=False)
        df.to_parquet(f"data/{self.parquet_file}")

        with open(f"data/{self.parquet_file}","rb") as filein:
            contents = filein.read()
        
        with self.output().open("w") as fileout:
            fileout.write(contents)


if __name__ == "__main__":
    luigi.build([PushNameBasics(),
    PushTitleAkas(),
    PushTitleBasics(),
    PushTitleCrew(),
    PushTitleEpisodes(),
    PushTitlePrincipals(),
    PushTitleRatings()], local_scheduler=True)
    # luigi.build([GetNameBasics(),
    #     GetTitleAkas(),
    #     GetTitleBasics(),
    #     GetTitleCrew(),
    #     GetTitleEpisodes(),
    #     GetTitlePrincipals(),
    #     GetTitleRatings()], local_scheduler=True)

