# Housing
This R package is for processing and analyzing housing authority data, including joining to the Medicaid data.

Much of the code is specific to the Seattle Housing Authority and King County Housing Authority datasets. However, many of the functions and concepts can be applied to other public housing authority (PHA) datasets (particularly those derived from 50058 HUD data).


- The R folder contains the functions used in the housing package.
- The man folder contains the help files for the housing package.
- The [ETL](https://github.com/PHSKC-APDE/housing_etl), [analyses](https://github.com/PHSKC-APDE/housing_analyses), and [HUD HEARS](https://github.com/PHSKC-APDE/hud_hears) code are now in distinct repositories. 


# Intructions for installing/updating the housing package
1) Make sure devtools is installed (type `install.packages("devtools")`)
2) Type `devtools::install_github("PHSKC-APDE/Housing", auth_token = NULL)`




