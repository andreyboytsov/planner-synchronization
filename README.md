# Planner Synchronizations
Just trying out the APIs. This project was developed for completely personal use, but anyway feel free to use it
(under the conditions of 3-clause BSD license provided in this directory). No liability accepted, use at your own
risk.

The goal is to backup and restore information of the different planning software. And later synchronize and
migrate between them when needed.

Supported software:
- ToodleDo
That's it for now :-Z

## Configuration
For each tool copy config-template.yaml to config_.yaml and change the fields as needed.
Sometimes you need to register application on the remote end in order to use API. 

## Run
Just run backup.py from any folder to save the results to CSVs. Restore/migrate - later.