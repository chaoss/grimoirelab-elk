# Studies
Studies create specific indices from RAW or enriched data. They usually extends the info available in standard indices
to deal with particular use cases. In fact, a given study could retrieve information from several indices or/and
create several output ones.

* [Areas of Code Study](#areas-of-code-study)
* [Onion Study](#onion-study)
* [Running studies from p2o](#running-studies-from-p2o)
* [Running studies from Mordred](#running-studies-from-mordred)

## Areas of Code Study
This study splits each commit in several items by touched file. It makes possible to explore metrics from a deeper
granularity. 

### Requirements
It expects to find an input index named `git_aoc-raw`, a git raw index containing 
data to apply this study on.

### Results
As output you will get an index following our [areas of code index fields convention](https://github.com/chaoss/grimoirelab-elk/blob/master/schema/areas_of_code.csv). This index will be named `git_aoc-enriched`.

Additionally, an alias named `git_areas_of_code` pointing to above's index is created if it doesn't exist.

## Onion Study
This study process information from an enriched index and computes Onion metric on that.

Onion model split contributors in three groups:
* **Core**: those contributing 80% of the activity (commits in this case). 
  These are the most committed developers, and those on which the project relies most.
* **Regular**: those contributing the next 15% of the activity. These are people committed
  to the project, and most likely to become part of the core group or maybe were already
  in it. The core and regular teams together account for 95% of the activity.
* **Casual**: those contributing the last 5% of the activity. There are people in
  the periphery of the project. However, they are important because it is very likely
  that future core and regular contributors will come out from this group.

In most models of FOSS development, where there are employees, they usually start directly
in regular or core, depending on their positions, experience and responsibilities in
the company. On the other hand, non-employees generally start as a part of the
casual group. Some of them will become regular and maybe core contributors as they gain
experience about the project.


Onion metric is calculated with different levels of granularity:
*  Globally: takes all data into account.
* By **organization**: splits data by organization.
* By **project**: split data by project.
* By **organization and project**: splits data by organization and project.

### Requirements:
It expects to find an input index named:
* **Git**:  `git_onion-src`, should be a git enriched index containing data to compute onion on.
* **GitHub Issues**: `github_issues_onion-src`, should be a GitHub enriched index containing only issues.
* **GitHub Pull Requests**: `github_prs_onion-src`, should be a GitHub enriched index containing only Pull Requests.

To allow using an index containing all GitHub information mixed, you must create the following aliases:
```
curl -XPOST https://user:pass@host/path_to_data/_aliases -d '
{
    "actions" : [
        {
            "add" : {
                 "index" : "github_issues",
                 "alias" : "github_issues_onion-src",
                 "filter" : {
                    "terms" : { 
                    "pull_request" : [
                        "false"
                        ]
                    }
                }
            }
        }
    ]
}'
```
```
curl -XPOST https://user:pass@host/path_to_data/_aliases -d '
{

"actions" : [
    {
        "add" : {
             "index" : "github_issues",
             "alias" : "github_prs_onion-src",
             "filter" : {
                "terms" : { 
                "pull_request" : [
                    "true"
                    ]
                }
            }
        }
    }
]
}'

``` 

These **indices should contain only those authors you want to compute onion on**. If needed, you can use a filtered
alias to exclude some of them. In a similar way as shown above for GitHub Issues and Pull Requests, you could
filtering out, for instance, **bots** and **empty commits** in Git.


### Results: 

As output you will get an index following our [onion index fields convention](https://github.com/chaoss/grimoirelab-elk/blob/master/schema/onion.csv).
This index will be named:
* **Git**: `git_onion-enriched`.
* **GitHub Issues**: `github_issues_onion-enriched`.
* **GitHub Pull Requests**: `github_prs_onion-enriched`.

Finally, an alias named `all_onion` is automatically created, including all of the above mentioned indices:
```
> GET _alias/all_onion

{
  "github_issues_onion-enriched": {
    "aliases": {
      "all_onion": {}
    }
  },
  "git_onion-enriched": {
    "aliases": {
      "all_onion": {}
    }
  },
  "github_prs_onion-enriched": {
    "aliases": {
      "all_onion": {}
    }
  }
}
```


## Running studies from p2o
Running studies from p2o is possible by using `--only-enrich` and `--only-studies` parameters. Other parameters
are needed to specify sortinghat database, ElasticSearch host, projects file path and data source.

Following line could be used (after adding the corresponding values instead of upper cased terms) to run git studies:

```
p2o --only-enrich --only-studies --db-sortinghat DBNAME --db-host DBHOST --db-user DBUSER --db-password DBPASSWORD --json-projects-map PROJECTS_PATH -e ES_HOST git ''
```

## Running studies from Mordred
Mordred has its own configuration [Mordred config documentation](https://github.com/chaoss/grimoirelab-mordred/blob/master/doc/config.md).

The studies are enabled per data source.

For executing Areas of Code Study and Onion studies for git:

```
[git]
raw_index = git-raw
enriched_index = git
studies = [enrich_areas_of_code, enrich_onion]
```
