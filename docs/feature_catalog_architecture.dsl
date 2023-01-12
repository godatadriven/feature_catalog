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

        feature_catalog = softwareSystem "Feature Catalog" "Definition and documentation and API to create features." {
            feature_groups = container "Feature Groups" "Definition and documentation of all features." "Python" {
                data_scientist -> this "Defines/develops features"
                zone = component "Zone Feature Group" "A group of related 'zone'." "Python"
                zone_likelyhood = component "Zone Likelyhood Feature Group" "A group of related 'zone likelyhood' features." "Python"
                zone_likelyhood -> zone "Depends on"
            }
            
            api = container "Feature Catalog API" "API used to create features." "Python" {                
                developer -> this "Maintains"
                model_a -> this "Calls API"
                model_b -> this "Calls API"
                notebook -> this "Calls API"
            }
        }        
    }

    views {
        systemContext feature_catalog {
            include *
            autolayout lr
        }

        container feature_catalog {
            include *
            autolayout lr
        }

        component feature_groups {
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