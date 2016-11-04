Functional tests for GrimoireELK adapted for GrimoireELK docker compose.

In experimental status yet, expect bugs and issues executing them.

Inside the gelk container:

* Create database for SortingHat:

```
mysqladmin -u root -h mariadb create test_sh
```

* Load projects data: 

```
mysqladmin -u root -h mariadb create test_projects
mysql -u root -h mariadb test_projects < test_projects.sql
```

* Configure the ElasticSearch connection in tests.conf
* Execute the tests: 
```
GrimoireELK/tests$ ./run_tests.py
```

You can execute specific tests. For example, to tests the enrichment with Sorting Hat:

    python3 test_backends.py TestBackends.test_enrich_sh

The current expected duration is more than five minutes.

Please, report issues inside the GitHub project.
