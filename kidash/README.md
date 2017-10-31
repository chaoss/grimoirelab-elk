# Kidash

Kidash is a prototype of a tool for managing Kibana dashboards from the command line. It is a part of [GrimoireLab](https://grimoirelab.github.io).

## Usage

Get a list of all options with:

```
$ kidash.py --help
```

For the names of the files containing panels definitions (JSON panel files),
kidash supports both importing them from local directories, of from the
`grimoirelab-panels` Python package, if installed. In fact, that package is
a dependency of kidash, which means that if you installed via pip, it will
always be present.

The algorith for finding a JSON panel file is, roughly:

* If the specified path (such as `panels/json/git.json` or `git.json`)
is found relative to the local directory, use it.
* If not found, if the specified path starts with `panels/json/`,
remove that part and look for the panel file in the `grimoirelab-panels`
package.
* If not found, look for the specified path directly in the
`grimoirelab-panels` package.

For example:

```
$ kidash.py --elastic_url-enrich http://localhost:9200 \
  --import git.json
```

will look for a file `git.json` in the current directory,
and if not found, for `git.json` in the `grimoirelab-panels` Python package,
if installed.
  
## Source code

The source code is for now a part of [GrimoireELK](https://github.com/grimoirelab/grimoireelk).
