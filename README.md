# Feature Logic

This is an example/template package to define and access features with a simple API.
This repo can be used as template or inspiration to create your own feature logic package.

This package assumes that the size of your data justifies the use of Spark.

## Why create a feature logic package?

By creating a package containing your feature logic you:

- define your features once
- allow re-use of features across teams and projects

By increasing collaboration you will get the following benefits:

- increase in speed of development (easy to re-use code)
- increase in reliability / quality of code (more contributors)

## How to use

### Install package

Install using `poetry install`.

### Download example data

```bash
curl https://storage.googleapis.com/shareddatasets/wow.parquet -o data/wow.parquet
```

### Run example notebook

After downloading the example data and installing the package you can run the example notebook `example_usage.ipynb`.
