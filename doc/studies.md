# Studies

## Onion Study

This study process information from a Git enriched index and computes Onion metric on that.

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
It expects to find an input index named `git_onion-src`, a git enriched index containing 
data to compute onion on.

### Results: 

Index named `git_onion-enriched` with following fields:

* project: project name.
* metadata__timestamp: when most recent item of current quarter was written in ElasticSearch (index time in source RAW index).
* timeframe: date corresponding to quarter start.
* author_org_name: organization name.
* onion_role: core, regular or casual.
* quarter: quarter as String following format YYYYQN.
* author_name: author name.
* author_uuid: author UUID from sortinghat.
* cum_net_sum: sum of commits used to computed onion.
* percent_cum_net_sum: percentage corresponding to above's field.
* metadata__enriched_on: index time in this index.
* unique_commits: number of commits made by the corresponding author.