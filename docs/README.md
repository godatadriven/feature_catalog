# Architecture overview

This folder contains the code to visualize the architecture using the [C4 model](https://c4model.com/).
It makes use of the [Structurizr DSL](https://structurizr.com/).

To create the diagrams you must have a structurizr account and then run
```
structurizr-cli push -w docs/feature_catalog_architecture.dsl -id <ID> -key <KEY> -secret <SECRET>
```
where you fill in the ID, KEY and SECRET from your account.

We compare the design of using only a Feature Catalog with another design where you also use a Feature Store.

## Only Feature Catalog

### Context
![C4 context diagram](images/structurizr-79513-FeatureCatalog-SystemContext.png)
![C4 context legend](images/structurizr-79513-FeatureCatalog-SystemContext-key.png)

### Container
![C4 container diagram](images/structurizr-79513-FeatureCatalog-Container.png)
![C4 container legend](images/structurizr-79513-FeatureCatalog-Container-key.png)

### Component
![C4 component diagram](images/structurizr-79513-FeatureCatalog-FeatureGroups-Component.png)
![C4 component legend](images/structurizr-79513-FeatureCatalog-FeatureGroups-Component-key.png)


## Including Feature Store

![C4 context diagram](images/structurizr-79513-FeaturePlatform-Container.png)
