# Project Overview

...


.. contents:: **Contents**
  :backlinks: none


Project Data & Scope
====================

...

Exploratory Data Analysis
=========================

...

Data Model
==========

...

Run ETL & Model Data
====================

Please read `Get Started <https://dvc.org/doc/get-started>`_ guide for a full version. Common workflow commands include:

+-----------------------------------+-------------------------------------------------------------------+
| Step                              | Command                                                           |
+===================================+===================================================================+
| Track data                        | | ``$ git add train.py``                                          |
|                                   | | ``$ dvc add images.zip``                                        |
+-----------------------------------+-------------------------------------------------------------------+
| Connect code and data by commands | | ``$ dvc run -d images.zip -o images/ unzip -q images.zip``      |
|                                   | | ``$ dvc run -d images/ -d train.py -o model.p python train.py`` |
+-----------------------------------+-------------------------------------------------------------------+
| Make changes and reproduce        | | ``$ vi train.py``                                               |
|                                   | | ``$ dvc repro model.p.dvc``                                     |
+-----------------------------------+-------------------------------------------------------------------+
| Share code                        | | ``$ git add .``                                                 |
|                                   | | ``$ git commit -m 'The baseline model'``                        |
|                                   | | ``$ git push``                                                  |
+-----------------------------------+-------------------------------------------------------------------+
| Share data and ML models          | | ``$ dvc remote add myremote -d s3://mybucket/image_cnn``        |
|                                   | | ``$ dvc push``                                                  |
+-----------------------------------+-------------------------------------------------------------------+v

Project Review & Write Up
=========================

Project Goal, Queries, Architecture, Model Justification
--------------------------------------------------------

...

tools and technologies Rationale
--------------------------------

...


Process Documentation
---------------------

...

Data Update Schedule
--------------------

...

Further Project Considerations
------------------------------

...




```bash
cd <project working directory>
python etl.py
```
