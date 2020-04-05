# insureData
insurance data pipelines


# Setup
```bash
> pip install conda
> cd $PROJECT_ROOT
> conda env create; conda activate insuredata-env
```

# Steps

We use [scrapy](https://scrapy.org/) to scrape EDGAR site. Set `$CRAWLER_ROOT` to be where `crawler` directory is

## Generate crawl seeds

Crawl all the states and cities links from [Yellow Pages Sitemap page](https://www.yellowpages.com/sitemap)
```bash
> cd $PRPJECT_ROOT/crawler
> scrapy crawl yp_locations -a statsFile=cities_stats.csv -a seedsFile=seeds.json
```

Next crawl each city

```bash
> scrapy crawl yp_insurance \
-a seedsFile='seeds/seeds.json' \
-a searchTerm=insurance \
-a statsFile=stats.json \
-a failedFile=failed.txt \
-o data.json
```
