workspace {

    model {
        developer = person "Developer" "Maintainer of the Feature Catalog" "External"
        data_scientist = person "Data Scientist" "Developer of machine learning models" "External"
        
        model_a = softwareSystem "An ML model in production." "External" {
            data_scientist -> this "Develops"
        }

        model_b = softwareSystem "Another ML model in production." "External" {
            data_scientist -> this "Develops"
        }

        notebook = softwareSystem "A data exploration environement." "External,Notebook" {
            data_scientist -> this "Experiments"
        }

        feature_catalog = softwareSystem "Feature Catalog" "Definition and documentation and API to create features." {
            feature_groups = container "Feature Groups" "Definition and documentation of all features." "Python" {
                data_scientist -> this "Develops"
                zone = component "Zone Feature Group" "A group of related 'zone'." "Python"
                zone_likelyhood = component "Zone Likelyhood Feature Group" "A group of related 'zone likelyhood' features." "Python"
                zone_likelyhood -> zone "Depends on"
            }
            
            api = container "Feature Catalog API" "API used to create features." "Python" {                
                developer -> this "Maintains"
                model_a -> this "Calls"
                model_b -> this "Calls"
                notebook -> this "Calls"
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
