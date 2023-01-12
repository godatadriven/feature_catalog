workspace {

    model {
        developer = person "Developer" "Maintainer of the Feature Catalog" "External"
        data_scientist = person "Data Scientist" "Developer of machine learning models" "External"
        
        model_a = softwareSystem "Model A" "An ML model in production." "External" {
            data_scientist -> this "Develops"
        }

        model_b = softwareSystem "Model B" "Another ML model in production." "External" {
            data_scientist -> this "Develops"
        }

        notebook = softwareSystem "Notebook environment" "A data exploration environement." "External,Notebook" {
            data_scientist -> this "Experiments"
        }

        feature_platform = softwareSystem "Feature Platform" "A platform to define, document, store and serve features." "External" {
            developer -> this "Maintains"            
            feature_store = container "Feature Store" "Physical storage of feature data." "Database" "External" {                
                model_a -> this "Reads from"
                model_b -> this "Reads from"
                notebook -> this "Reads from"
                developer -> this "Maintains"
            }
            feature_catalog = container "Feature Catalog" "Definition, documentation and API to create features." {
                feature_store -> this "Periodically calls API"
                data_scientist -> this "Defines/develops features"
                developer -> this "Maintains"
                notebook -> this "Calls API"
            }
        }   
    }

    views {
        systemContext feature_platform {
            include *
            autolayout lr
        }

        container feature_platform {
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
