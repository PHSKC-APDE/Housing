# Housing
This R package is for processing and analyzing housing authority data, including joining to the Medicaid data.

Much of the code is specific to the Seattle Housing Authority and King County Housing Authority datasets. However, many of the functions and concepts can be applied to other public housing authority (PHA) datasets (particularly those derived from 50058 HUD data).


- See processing folder for code that generates a combined data set.
- See analyses folder for code that analyzes the PHA data.
- The R folder contains the functions used in the housing package.
- The man folder contains the help files for the housing package.
- The KCHA and SHA folders contain code for analyses specific to that PHA.


# Intructions for installing/updating the housing package
1) Make sure devtools is installed (type install.packages("devtools"))
2) Type devtools::install_github("PHSKC-APDE/Housing")




