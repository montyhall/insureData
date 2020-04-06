# insureData
insurance data pipelines


# Setup
```bash
> pip install conda
> cd $PROJECT_ROOT
> conda env create; conda activate insuredata-env
```

Then follow [these steps](https://gist.github.com/DusanMadar/8d11026b7ce0bce6a67f7dd87b999f6b) to install
* `tor`, `privoxy` `TorIpChanger` 
        
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