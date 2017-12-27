# Welcome to GrimoireELK [![Build Status](https://travis-ci.org/grimoirelab/GrimoireELK.svg?branch=master)](https://travis-ci.org/grimoirelab/GrimoireELK)

GrimoireELK is an evolving prototype of the *Grimoire Open Development Analytics platform*. 

Tutorials on howto use it are published in [GrimoireLab Tutorial](https://grimoirelab.gitbooks.io/tutorial).

## Packages

The following packages are produced from this repository:

* `grimoire-elk`: [![PyPI version](https://badge.fury.io/py/grimoire-elk.svg)](https://badge.fury.io/py/grimoire-elk)

* `grimoire-kidash`: [![PyPI version](https://badge.fury.io/py/grimoire-kidash.svg)](https://badge.fury.io/py/grimoire-kidash)

`grimoire-elk` admits some extras, when installing: `arthur`
(for installing also `grimoirelab-arthur` package)
and `sortinghat` (for installing also `sortinghat` package).
You can specify that you want to install those extras as follows:

```
% pip install "grimoire-elk[sortinghat]"
% pip install "grimoire-elk[arthur]"
```
