workspace {

    model {
        developer = person "Developer" "Maintainer of ML platform and Feature Catalog" "External"
        data_scientist = person "Data Scientist" "Developer of ML models and features" "External"

        notebook = softwareSystem "Notebook environment" "A data exploration environement." "External,Notebook" {
            data_scientist -> this "Experiments"
        }

        ml_platform = softwareSystem "ML platform (Feature Store)" "A platform on which ML products are built and deployed." "External" {
            developer -> this "Maintains"

            model_pipeline_a = container "Model Pipeline A" "An ML model in production." "Python" "External"{
                data_scientist -> this "Develops model"
            }

            model_pipeline_b = container "Model Pipeline B" "Another ML model in production." "Python" "External" {
                data_scientist -> this "Develops model"
            }            

            feature_store = container "Feature Store" "Physical storage of feature data." "Data store" "External" {                
                model_pipeline_a -> this "Reads from"
                model_pipeline_b -> this "Reads from"
                notebook -> this "Reads from"
                developer -> this "Maintains"
            }

            feature_catalog = container "Feature Catalog" "Definition, documentation and API to create features." "Git repo" {
                feature_store -> this "Periodically calls API"
                data_scientist -> this "Defines/develops features"
                developer -> this "Maintains"
                notebook -> this "Calls Feature Catalog API"
            }
        }   
    }

    views {
        systemContext ml_platform {
            include *
            autolayout lr
        }

        container ml_platform {
            include *
            autolayout lr
        }

        theme default
        
        styles {
            element "External" {
                background #cccccc
            }
            element "Notebook" {
                shape WebBrowser
            }
        }
    }
}
