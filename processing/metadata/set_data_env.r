# Function to set data file variable paths and names in working enviroment 
# inputs: "sha_data", "kcha_data"

 set_data_envr = function(METADATA,data_source) {
    lapply(seq_along(METADATA[[data_source]]), 
           function(x) {
            assign(names(METADATA[[data_source]][x]), METADATA[[data_source]][[x]], envir=.GlobalEnv)
        }
    )
 }
 


