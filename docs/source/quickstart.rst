Quickstart
==========

``intake-splunk`` provides quick and easy access to tabular data stored in
Apache `Splunk`_

.. _Splunk: https://www.splunk.com/

This plugin reads splunk query results without random access: there is only ever
a single partition.

Installation
------------

To use this plugin for `intake`_, install with the following command::

   conda install -c intake intake-splunk

.. _intake: https://github.com/ContinuumIO/intake

Usage
-----

Ad-hoc
~~~~~~

After installation, the function ``intake.open_splunk``
will become available. It can be used to execute queries on the splunk
server, and download the results as a list of dictionaries.

Three parameters are of interest when defining a data source:

- query: the query to execute, using Splunk's `Query Syntax`_


.. _Query Syntax:http://docs.splunk.com/Documentation/Splunk/7.0.2/Search/Aboutsearchlanguagesyntax

Creating Catalog Entries
~~~~~~~~~~~~~~~~~~~~~~~~

To include in a catalog, the plugin must be listed in the plugins of the catalog::

   plugins:
     source:
       - module: intake_splunk

and entries must specify ``driver: splunk``.



Using a Catalog
~~~~~~~~~~~~~~~

