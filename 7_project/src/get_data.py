import luigi
import wget
from datetime import datetime

class GetNameBasics(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/name_basics.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/name.basics.tsv.gz", out="data/name_basics.tsv.gz")
    

class GetTitleAkas(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/title_akas.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/title.akas.tsv.gz", out="data/title_akas.tsv.gz")


class GetTitleBasics(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/title_basics.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/title.basics.tsv.gz", out="data/title_basics.tsv.gz")


class GetTitleCrew(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/title_crew.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/title.crew.tsv.gz", out="data/title_crew.tsv.gz")


class GetTitleEpisodes(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/title_episodes.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/title.episode.tsv.gz", out="data/title_episodes.tsv.gz")


class GetTitlePrincipals(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/title_principals.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/title.principals.tsv.gz", out="data/title_principals.tsv.gz")


class GetTitleRatings(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/title_ratings.tsv.gz")
    
    def run(self):
        wget.download("https://datasets.imdbws.com/title.ratings.tsv.gz", out="data/title_ratings.tsv.gz")


if __name__ == "__main__":
    luigi.build([GetNameBasics(),
        GetTitleAkas(),
        GetTitleBasics(),
        GetTitleCrew(),
        GetTitleEpisodes(),
        GetTitlePrincipals(),
        GetTitleRatings()], local_scheduler=True)

