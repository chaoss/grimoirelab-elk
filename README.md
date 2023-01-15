# Welcome to GrimoireELK [![Build Status](https://github.com/chaoss/grimoirelab-elk/workflows/tests/badge.svg)](https://github.com/chaoss/grimoirelab-elk/actions?query=workflow:tests+branch:master+event:push) [![Coverage Status](https://coveralls.io/repos/github/chaoss/grimoirelab-elk/badge.svg?branch=master)](https://coveralls.io/github/chaoss/grimoirelab-elk?branch=master) [![PyPI version](https://badge.fury.io/py/grimoire-elk.svg)](https://badge.fury.io/py/grimoire-elk) 

GrimoireELK is the component of [GrimoireLab](https://github.com/chaoss/grimoirelab) that interacts with the ElasticSearch database. Its goal is two-fold, first it aims at offering a convenient
way to store the data coming from Perceval, second it processes and enriches the data in a format that can be consumed by Kibiter.

The [Perceval](https://github.com/chaoss/grimoirelab-perceval) data is stored in ElasticSearch indexes as raw documents (one per item extracted by Perceval). Those raw documents, which will be referred to as "raw data" in
this documentation, include all information coming from the original data source which grants the platform to perform multiple analysis without the need of downloading the
same data over and over again. Once raw data is retrieved, a new phase starts where data is enriched according to the data source from where it was collected and stored
in ElasticSearch indexes. The enrichment removes information not needed by Kibiter and includes additional information which is not directly available within the raw data.
For instance, pair programming information for Git data, time to solve (i.e., close or merge) issues and pull requests for GitHub data, and identities and organization
information coming from [SortingHat](https://github.com/chaoss/grimoirelab-sortinghat) . The enriched data is stored as JSON documents, which embed information linked to
the corresponding raw documents to ease debugging and guarantee traceability.

## Raw data

Each raw document stored in an ElasticSearch index contains a set of common first level fields, regardless of the data source:
- **backend** (string): Name of the Perceval backend used to retrieve the information.
- **backend_version** (string): Version of the abovementioned backend.
- **perceval_version** (string): Perceval version.
- **timestamp** (long): When the item was retrieved by Perceval (in epoch format).
- **origin** (string): Where the item was retrieved from.
- **uuid** (string): Item unique identifier.
- **updated_on** (long): When the item was updated in the original source (in epoch format).
- **classified_fields_filtered** (list): List of data field names (strings) which  contained classified information and that were removed from the original item. Depends on activating ‘--filter-classified’ flag in Perceval.
- **category** (string): Type of the items to fetch (commit, pull request, etc.) depending on the data source.
- **tag** (string): Custom label that can be set in Perceval for each retrieval.
- **data** (object): This field contains a copy in JSON format of the original data as it is retrieved from the data source. Next sections will describe where GrimoireLab get this information from.

## Enriched data

Each enriched index includes one or more types of documents, which are summarized below.

- **Askbot**: each document can be either a question, an answer or answer's comments.
- **Bugzilla**: each document corresponds to a single issue (fetched using CGI calls).
- **Bugzillarest**: each document corresponds to a single issue (fetched using Bugzilla REST API).
- **Cocom**: each document corresponds to single file in a commit, with code complexity information.
- **Colic**: each document corresponds to single file in a commit, with license information.
- **Confluence**: each document can be either a new page, a page edit, a comment or an attachment.
- **Crates**: each document corresponds to an event.
- **Discourse**: each document can be either a question or an answer.
- **Dockerhub**: each document corresponds to an image.
- **Finosmeetings**: each document corresponds to details about a meeting.
- **Functest**: each document corresponds to details about a test.
- **Gerrit**: each document can be either a changeset, a comment, a patchset or a patchset approval.
- **Git**: each document corresponds to a single commit.
- **Git Areas of Code**: each document corresponds to one single file.
- **GitHub issues**: each document corresponds to an issue.
- **GitHub pull requests**: each document corresponds to a pull request.
- **GitHub repo statistics**: each document includes repo statistics (e.g., forks, watchers).
- **GitLab issues**: each document corresponds to an issue.
- **GitLab merge requests**: each document corresponds to a merge request.
- **Gitter**: each document corresponds to a message.
- **Googlehits**: each document contains hits information derived from Google.
- **Groupsio**: each document corresponds to a message.
- **Hyperkitty**: each document corresponds to a message.
- **Jenkins**: each document corresponds to a single built.
- **Jira**: each document corresponds to an issue or a comment. To simplify counting user activities, issues are duplicated and they can include assignee, reporter and creator data respectively.
- **Kitsune**: each document can be either a question or an answer.
- **Launchpad**: each document corresponds to a bug.
- **Mattermost**: each document corresponds to a message.
- **Mbox**: each document corresponds to a message.
- **Mediawiki**: each document corresponds to a review.
- **Meetup**: each document can be either an event, a rsvp or a comment.
- **Mozillaclub**: each document includes event information.
- **Nttp**: each document corresponds to a message.
- **Onion Study/Community Structure**: each document corresponds to an author in a specific quarter, split by organization and project. That means we have an entry for the author’s overall contributions in a given quarter, one entry for the author in each one of the projects he contributed to in that quarter and the same for the author in each of the organizations he is affiliated to in that quarter. This way we store results of onion analysis computed overall, by project and by organization
- **Pagure**: each document corresponds to an issue.
- **Phabricator**: each document corresponds to a task.
- **Pipermail**: each document corresponds to a message.
- **Puppetforge**: each document corresponds to a module.
- **Rocketchat**: each document corresponds to a message.
- **Redmine**: each document corresponds to an issue.
- **Remo activities**: each document corresponds to an activity.
- **Remo events**: each document corresponds to an event.
- **Remo users**: each document corresponds to a user.
- **Rss**: each document corresponds to an entry.
- **Slack**: each document corresponds to a message.
- **Stackexchange**: each document can be either a question or answer.
- **Supybot**: each document corresponds to a message.
- **Telegram**: each document corresponds to a message.
- **Twitter**: each document corresponds to a tweet.

### Fields
Each enriched document contains a set of fields, they can be (i) common to all data sources (e.g., metadata fields, time field), (ii) specific to the data source, (iii) related to contributor’s
profile information (i.e., identity fields) or (iv) to the project listed in the Mordred projects.json (i.e., project fields).

#### Metadata fields
- **metadata__timestamp** (date): Date when the item was retrieved from the original data source and stored in the index with raw documents.
- **metadata__updated_on** (date): Date when the item was updated in its original data source.
- **metadata__enriched_on** (date): Date when the item was enriched and stored in the index with enriched documents.
- **metadata__gelk_backend_name** (string): Name of the backend used to enrich information.
- **metadata__gelk_version** (string): Version of the backend used to enrich information.
- **origin** (string): Original URL where the repository was retrieved from.

#### Identity fields
- **author_uuid** (string): Author profile unique identifier. Used for counting authors and cross-referencing data among data sources in ElasticSearch and between ElasticSearch, SortingHat and Hatstall.
- **author_org_name** (string): Organization name to which the author is affiliated to. Same author could have different affiliations based on non-overlapping time periods. Used for aggregating contributors and contributions by organization.
- **author_name** (string): Similar to author_uuid, but less useful for unique counts as different profiles could share the same name. Nevertheless is more appropriate to show this field when aggregating data by author as it is usually nicer to see a name than a hash value.
- **author_bot** (boolean): True if the given author is identified as a bot.
- **author_domain** (string): Domain associated to the author in SortingHat profile.
- **author_id** (string): Author identifier. This id comes from SortingHat and identifies each different identity provided by SortingHat. These identifiers are grouped in a single author_uuid, so this fields is not commonly used unless data needs to be debugged.

#### Project fields
- **project** (string): Project name as defined in the JSON file where repositories are grouped by project.
- **project_1** (string): Project (if more than one level is allowed in project hierarchy).

#### Time field:
- **grimoire_creation_date** (date): Date when the item was created upstream. Used by default to represent data in time series on the dashboards.

#### Demography fields:
- **author_max_date** (date): Date of most recent commit made by this author.
- **author_min_date** (date): Date of the first commit made by this author.

#### Extra fields:
- **extra_** (anything): Extra fields added using the `enrich_extra_data` study.

#### Data source specific fields
Details of the fields of each data source is available in the [Schema](https://github.com/chaoss/grimoirelab-elk/tree/master/schema) folder.

## Installation

There are several ways to install GrimoireELK on your system: packages or source 
code using Poetry or pip.

### PyPI

GrimoireELK can be installed using pip, a tool for installing Python packages. 
To do it, run the next command:
```
$ pip install grimoire-elk
```

### Source code

To install from the source code you will need to clone the repository first:
```
$ git clone https://github.com/chaoss/grimoirelab-elk
$ cd grimoirelab-elk
```

Then use pip or Poetry to install the package along with its dependencies.

#### Pip
To install the package from local directory run the following command:
```
$ pip install .
```
In case you are a developer, you should install GrimoireELK in editable mode:
```
$ pip install -e .
```

#### Poetry
We use [poetry](https://python-poetry.org/) for dependency management and 
packaging. You can install it following its [documentation](https://python-poetry.org/docs/#installation).
Once you have installed it, you can install GrimoireELK and the dependencies in 
a project isolated environment using:
```
$ poetry install
```
To spaw a new shell within the virtual environment use:
```
$ poetry shell
```

## Running tests

Tests are located in the folder [tests](https://github.com/chaoss/grimoirelab-elk/tree/master/tests). 
In order to run them, you need to have in your machine instances (or Docker containers) of ElasticSearch and MySQL

Then you need to:
- update the file [tests.conf](https://github.com/chaoss/grimoirelab-elk/blob/master/tests/tests.conf) file:
  - in case your ElasticSearch instance isn't available at `http://localhost:9200`. For example, if you are using the secure edition of elasticsearch, it will be located at `https://admin:admin@localhost:9200`
  - in case you are using non-default credentials for your SortingHat database, you will need to include the `[Database]` section of the file with both `user` and `password` parameters
- create the databases `test_sh` and `test_projects` in your MySQL instance (e.g., `mysql -u root -e "create database test_sh"`, if you are running mysql in a container use `docker exec -i <container id> mysql -u root -e "create database test_sh"`)
- populate the database `test_projects` with the SQL file [test_projects.sql](https://github.com/chaoss/grimoirelab-elk/blob/master/tests/test_projects.sql) (e.g., `mysql -u root test_projects < tests/test_projects.sql`)

The full battery of tests can be executed with [run_tests.py](https://github.com/chaoss/grimoirelab-elk/blob/master/tests/run_tests.py). However, it is also possible to execute
a sub-set of tests by running the single test files (`test_*` files in the [tests folder](https://github.com/chaoss/grimoirelab-elk/tree/master/tests))

The tests can be run in combination with the Python package `coverage`. The steps below show how to do it:
```buildoutcfg
$ pip3 install coveralls
$ cd <path-to-ELK>/tests
$ python3 -m coverage run run_tests.py --source=grimoire_elk 
```

![pycharm-config-run_tests](https://user-images.githubusercontent.com/25265451/75114992-d9e9c400-5680-11ea-8b8e-9c50569367a4.png "pycharm-config-run_tests")

Coverage will generate a file `.coverage` in the tests folder, which can be inspected with the following command:
```buildoutcfg
cd <path-to-ELK>/tests
python3 -m coverage report -m
```

![pycharm-config_report](https://user-images.githubusercontent.com/25265451/75115046-554b7580-5681-11ea-92b4-b20c2ece1283.png "pycharm-config_report")

The output will be similar to the following one:
```buildoutcfg
Name                                                                                                                Stmts   Miss  Cover   Missing
--------------------------------------------------------------------------------------------------------------------------------------------------
.../ELK/grimoire_elk/__init__.py                                                                                       4      0   100%
.../ELK/grimoire_elk/_version.py                                                                                       1      0   100%
```
