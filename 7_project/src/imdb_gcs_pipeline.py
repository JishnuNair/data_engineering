import luigi
import wget
import gzip
from datetime import datetime
from luigi.contrib import gcs
from luigi.format import Nop

class GetNameBasics(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/name_basics.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/name.basics.tsv.gz", out="data/name_basics.tsv.gz")
    

class PushNameBasics(luigi.Task):

    def requires(self):
        return GetNameBasics()

    def output(self):
        return gcs.GCSTarget("gs://dtc_de_imdb/raw/name_basics.tsv", format=Nop)

    def run(self):

        with gzip.open("data/name_basics.tsv.gz","rb") as filein:
            contents = filein.read()

        with self.output().open("w") as fileout:
            fileout.write(contents)


class GetTitleAkas(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/title_akas.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/title.akas.tsv.gz", out="data/title_akas.tsv.gz")


class PushTitleAkas(luigi.Task):

    def requires(self):
        return GetTitleAkas()

    def output(self):
        return gcs.GCSTarget("gs://dtc_de_imdb/raw/title_akas.tsv", format=Nop)

    def run(self):

        with gzip.open("data/title_akas.tsv.gz","rb") as filein:
            contents = filein.read()

        with self.output().open("w") as fileout:
            fileout.write(contents)


class GetTitleBasics(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/title_basics.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/title.basics.tsv.gz", out="data/title_basics.tsv.gz")


class PushTitleBasics(luigi.Task):

    def requires(self):
        return GetTitleBasics()

    def output(self):
        return gcs.GCSTarget("gs://dtc_de_imdb/raw/title_basics.tsv", format=Nop)

    def run(self):

        with gzip.open("data/title_basics.tsv.gz","rb") as filein:
            contents = filein.read()

        with self.output().open("w") as fileout:
            fileout.write(contents)


class GetTitleCrew(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/title_crew.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/title.crew.tsv.gz", out="data/title_crew.tsv.gz")


class PushTitleCrew(luigi.Task):

    def requires(self):
        return GetTitleCrew()

    def output(self):
        return gcs.GCSTarget("gs://dtc_de_imdb/raw/title_crew.tsv", format=Nop)

    def run(self):

        with gzip.open("data/title_crew.tsv.gz","rb") as filein:
            contents = filein.read()

        with self.output().open("w") as fileout:
            fileout.write(contents)


class GetTitleEpisodes(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/title_episodes.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/title.episode.tsv.gz", out="data/title_episodes.tsv.gz")


class PushTitleEpisodes(luigi.Task):

    def requires(self):
        return GetTitleEpisodes()

    def output(self):
        return gcs.GCSTarget("gs://dtc_de_imdb/raw/title_episodes.tsv", format=Nop)

    def run(self):

        with gzip.open("data/title_episodes.tsv.gz","rb") as filein:
            contents = filein.read()

        with self.output().open("w") as fileout:
            fileout.write(contents)


class GetTitlePrincipals(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/title_principals.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/title.principals.tsv.gz", out="data/title_principals.tsv.gz")


class PushTitlePrincipals(luigi.Task):

    def requires(self):
        return GetTitlePrincipals()

    def output(self):
        return gcs.GCSTarget("gs://dtc_de_imdb/raw/title_principals.tsv", format=Nop)

    def run(self):

        with gzip.open("data/title_principals.tsv.gz","rb") as filein:
            contents = filein.read()

        with self.output().open("w") as fileout:
            fileout.write(contents)


class GetTitleRatings(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/title_ratings.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/title.ratings.tsv.gz", out="data/title_ratings.tsv.gz")


class PushTitleRatings(luigi.Task):

    def requires(self):
        return GetTitleRatings()

    def output(self):
        return gcs.GCSTarget("gs://dtc_de_imdb/raw/title_ratings.tsv", format=Nop)

    def run(self):

        with gzip.open("data/title_ratings.tsv.gz","rb") as filein:
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

