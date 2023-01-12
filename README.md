# Feature Catalog

<img src="docs/images/complex_pipelines.jpg" width="100" style="float:left; margin-right:20px;" />

This example code base can be used as template or inspiration to create your own Feature Catalog. It contains some example code to define your features and expose them to users via a simple API. We assume that the size of your data justifies the use of Spark.

Below you find more information about what a Feature Catalog is, why to create it and what the difference is with a Feature Store.

## What is a Feature Catalog?

A Feature Catalog is a place where you define and document your features such that they can be created via an API.

Note that this not (yet) includes the storage of the features in a Feature Store. A Feature Catalog already gives you a lot of benefits without the complexity of a Feature Store or a full Feature Platform. Also see the Architecture section about this difference.

## Why create a Feature Catalog?

By creating a Feature Catalog you:

- define your features once (single source of truth)
- allow re-use of features across teams and projects (better collaboration)

By increasing collaboration you will get the following benefits:

- increase in speed of development (easy to re-use code)
- increase in reliability / quality of code (more contributors)

<img src="docs/images/feature_catalog.png" width="600"/>

## Architecture

The full architecture can be found in the docs folder, but here is the main overview:

<img src="docs/images/structurizr-79513-MLplatform-Container.png" width="600"/>

<img src="docs/images/structurizr-79513-MLplatform-Container-key.png" width="600"/>

### Including a Feature Store
When including a Feature Store it could look somewhat like this:

<img src="docs/images/structurizr-79513-MLplatformFeatureStore-Container.png" width="600"/>

## How to use

### Install package

Install using `poetry install`.

### Download example data

```bash
curl https://storage.googleapis.com/shareddatasets/wow.parquet -o data/wow.parquet
```

### Run example notebook

After downloading the example data and installing the package you can run the example notebook `example_usage.ipynb`.
