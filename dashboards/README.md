Dashboard templates to be used to create data source dashboards using GrimoireELK platform.

To export a dashboard from a Kibana working with elasticsearch:9200:

```
GrimoireELK/utils/kidash.py -e http://elasticsearch:9200 -g --export gerrit-activity.json --dashboard Gerrit-Activity
```

To import a dashboard in Kibana working with elasticsearch:9200:

```
GrimoireELK/utils/kidash.py -e http://elasticsearch:9200 -g --import gerrit-activity.json 
```

To use the dashboard as template for generating a new dashboard using gerrit_review.openstack.org_enrich as index:

```
GrimoireELK/utils/e2k.py -g -e http://elasticsearch:9200 -d "Gerrit-Activity" -i gerrit_review.openstack.org_enrich
```

