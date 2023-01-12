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

### Context overview

<img src="images/structurizr-79513-MLplatform-SystemContext.png" width="600"/>

<img src="images/structurizr-79513-MLplatform-SystemContext-key.png" width="600"/>

### Container overview

<img src="images/structurizr-79513-MLplatform-Container.png" width="600"/>

<img src="images/structurizr-79513-MLplatform-Container-key.png" width="600"/>

### Component overview

<img src="images/structurizr-79513-MLplatform-FeatureCatalog-Component.png" width="600"/>

<img src="images/structurizr-79513-MLplatform-FeatureCatalog-Component-key.png" width="600"/>


## Including Feature Store
### Container overview

<img src="images/structurizr-79513-MLplatformFeatureStore-Container.png" width="600"/>
