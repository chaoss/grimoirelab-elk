# Data Models

This directory contains the data model used for each of the
standard panels and indexes in GrimoireLab.

The goal detailing the CSV and JSON files is to have a
centralized place where everyone can look at as the enrichment
toolchain.

Each of the CSV files contains two main columns: name and type.
The type is directly taken from ElasticSearch and used as
standard to define the several types available. If any other
database engine is required, there should be a migration
to that type name space.

Others could be added in the future for extra needs. For example
there is a need to specify if a field is aggregatable (using
ElasticSearch terminology) or to describe the field.

## Testing

As a side activity, this CSV files are expected to help in 
testing activities at any point. First as these define the
expected output of the enrichment phase and contain the minimum
fields required to produce a proper panel with the GrimoireLab
tool set.

