Flow Info
=========

This is a simple package for downloading logs from Globus Flows, and doing some light analysis on them.

Usage
-----

.. warning::
    This package is still in early development and will change over time. Expect things like config files and commands to change
    around as we figure out the best way of doing things.

1. Clone this package locally.
2. Edit beamlines.cfg with your particular beamlines
3. Set beamlines -> current_app to your app name. We don't actually have a way to do this yet via the command!

Beamline commands should now work. You should start by running ``flow-info update`` to start fetching logs!

Some other options per-app beamlines.cfg include:

    * name -- Determines the name of logs generated
    * client_id -- The client_id to use. 
    * client_type -- Can be one of "user" or "confidential-client". Determines how logs are fetched. client-credentials require exporting a client secret on the cmd
    * filter_run_tags -- A comma separated list of tags to filter by run. Useful in "user" mode.
    * flow_owner -- Filter flows based on owner. 
    * flow_order -- A list of states in order they should be displayed on graphs.
