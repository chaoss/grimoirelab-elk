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
It expects to find an input index named `git_aoc-raw`, a git enriched index containing 
data to compute onion on.

### Results
Index named `git_aoc-enriched` with following fields:

* addedlines
* author_bot
* author_domain
* author_id
* author_name
* author_org_name
* author_user_name
* author_uuid
* committer
* committer_date
* date
* eventtype
* file_dir_name: path in which the file is located, not including file name.
* file_ext: file extension.
* file_name: file name with extension.
* file_path_list: list of splitted path parts.
* fileaction: action performed by the commit over the file.
* filepath: complete file path.
* files: number of files touched by the same commit this file is included in.
* filetype: `Code` or `Other`, based on file extension.
* git_author_domain
* grimoire_creation_date
* hash
* id
* message
* metadata__enriched_on
* metadata__timestamp
* metadata__updated_on
* owner
* perceval_uuid
* project
* project_1
* removedlines
* repository

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


## Running studies from p2o
Running studies from p2o is possible by using `--only-enrich` and `--only-studies` parameters. Other parameters
are needed to specify sortinghat database, ElasticSearch host, projects file path and data source.

Following line could be used (after adding the corresponding values instead of upper cased terms) to run git studies:

```
p2o --only-enrich --only-studies --db-sortinghat DBNAME --db-host DBHOST --db-user DBUSER --db-password DBPASSWORD --json-projects-map PROJECTS_PATH -e ES_HOST git ''
```

## Running studies from Mordred
Mordred has its own configuration to enable studies. Please check [Mordred config documentation](https://github.com/chaoss/grimoirelab-mordred/config.md).