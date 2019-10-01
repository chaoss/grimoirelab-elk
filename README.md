# Welcome to GrimoireELK [![Build Status](https://travis-ci.org/chaoss/grimoirelab-elk.svg?branch=master)](https://travis-ci.org/chaoss/grimoirelab-elk)[![Coverage Status](https://coveralls.io/repos/github/chaoss/grimoirelab-elk/badge.svg?branch=master)](https://coveralls.io/github/chaoss/grimoirelab-elk?branch=master)

GrimoireELK is the component that interacts with the ElasticSearch database. Its goal is two-fold, first it aims at offering a convenient 
way to store the data coming from Perceval, second it processes and enriches the data in a format that can be consumed by Kibiter.

The Perceval data is stored in ElasticSearch indexes as raw documents (one per item extracted by Perceval). Such a raw data includes all information coming from the original 
data sources, thus granting the platform to perform multiple analysis without the need of downloading the same data over and over. The raw data is then enriched according to the 
data source from where it was collected and stored to enriched indexes. The enrichment removes information not needed by Kibiter and includes additional information which is 
not directly available within the raw data. For instance, pair programming information for Git data, time to solve (i.e., close or merge) issues and pull requests for GitHub 
data, and identities and organization information coming from [SortingHat](https://github.com/chaoss/grimoirelab-sortinghat) . The enriched data is stored as JSON 
documents, which embed information linked to the corresponding raw documents to ease debugging and guarantee traceability.

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
- **Bugzilla**: each document corresponds to a single issue.
- **Bugzillarest**: each document corresponds to a single issue.
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
- **Googlehits**: each document contains hits information derived from Google.
- **Groupsio**: each document corresponds to a message.
- **Hyperkitty**: each document corresponds to a message.
- **Jenkins**: each document corresponds to a single built.
- **Jira**: each document corresponds to an issue or a comment. To simplify counting user activities, issues are duplicated and they can include assignee, reporter and creator data respectively.
- **Kitsune**: each document can be either a question or an answer.
- **Mattermost**: each document corresponds to a message.
- **Mbox**: each document corresponds to a message.
- **Mediawiki**: each document corresponds to a review.
- **Meetup**: each document can be either an event, a rsvp or a comment.
- **Mozillaclub**: each document includes event information.
- **Nttp**: each document corresponds to a message.
- **Onion Study/Community Structure**: each document corresponds to an author in a specific quarter, split by organization and project. That means we have an entry for the author’s overall contributions in a given quarter, one entry for the author in each one of the projects he contributed to in that quarter and the same for the author in each of the organizations he is affiliated to in that quarter. This way we store results of onion analysis computed overall, by project and by organization
- **Phabricator**: each document corresponds to a task.
- **Pipermail**: each document corresponds to a message.
- **Puppetforge**: each document corresponds to a module.
- **Redmine**: each document corresponds to an issue.
- **Remo activities**: each document corresponds to an activity.
- **Remo events**: each document corresponds to an event.
- **Remo users**: each document corresponds to a user.
- **Rss**: each document corresponds to an entry.
- **Slack**: each document corresponds to a message.
- **Stackexchange**: each document can be either a question or answer.
- **Supybot**: each document corresponds to a message.
- **Telegram**: each document corresponds to a message.
- **Telegram**: each document corresponds to a tweet.

### Fields
Each enriched document contains a set of fields, they can be (i) common to all data sources (e.g., metadata fields, time field), (ii) specific to the data source, (iii) related to contributor’s 
profile information (i.e., identity fields) or (iv) to the project listed in the Mordred projects.json (i.e., project fields).

#### Metadata fields
- **metadata__timestamp** (date): Date when the item was retrieved from the original data source.
- **metadata__updated_on** (date): Date when the item was updated in its original data source.
- **metadata__enriched_on** (date): Date when the item was enriched.
- **metadata__gelk_backend_name** (string): Name of the backend used to enrich information.
- **metadata__gelk_version** (string): Version of the backend used to enrich information.
- **origin** (string): Original URL where the repository was retrieved from.

#### Identity fields
- **author_uuid** (string): Author profile unique identifier. Used for counting authors and cross-referencing data among data sources in ElasticSearch and between ElasticSearch, SortingHat and Hatstall.
- **author_org_name** (string): Organization name to which the author is affiliated to. Same author could have different affiliations based on non-overlapping time periods. Used for aggregating contributors and contributions by organization.
- **author_name** (string): Similar to author_uuid, but less useful for unique counts as different profiles could share the same name. Nevertheless is more appropriate to show this field when aggregating data by author as it is usually nicer to see a name than a hash value.
- **author_bot** (boolean): True if the given author is identified as a bot.
- **author_domain** (string): Domain associated to the author in SortingHat profile.
- **author_id** (string): Author Id from SortingHat.

#### Project fields
- **project** (string): Project.
- **project_1** (string): Project (if more than one level is allowed in project hierarchy).

#### Time field:
- **grimoire_creation_date** (date): Date when the item was created upstream. Used to represent data in time series by default in the dashboards.

#### Demography fields:
- **author_max_date** (date): Date of most recent commit made by this author.
- **author_min_date** (date): Date of the first commit made by this author.

#### Data source specific fields
Details of the fields of each data source is available in the [Schema](https://github.com/chaoss/grimoirelab-elk/tree/master/schema) folder.

