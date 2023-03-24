# Systematically convert XLS to CSV files
# Import all columns as text to prevent R from mis-identifying a column and then
# dropping data. Will then make decisions on what to keep or drop in R/SQL ETL

# generalized ----
library(readxl)
setwd("//phdata01/DROF_Data/DOH DATA/Housing/KCHA/Original_data/new/")

myfiles <- grep(".xls", list.files(), value = T)

for(i in myfiles){
  newname <- gsub(".xls$|.xlsx$", ".csv", i)
  blah <- readxl::read_excel(i, col_types = 'text') 
  write.csv(blah, newname, row.names = F)
  warnings()
}

# SHA 2022 ----
sha2022 <- readxl::read_excel("//phdata01/DROF_Data/DOH DATA/Housing/SHA/Original_data/2022 data extract/2022_sha_hcv_ph_complete_received_2023_03_14.xlsx", 
                              col_types = 'text', 
                              sheet = 1)
write.csv(sha2022, file = "//phdata01/DROF_Data/DOH DATA/Housing/SHA/Original_data/sha_hcv_ph_2022.csv", 
          row.names = FALSE)

# SHA 2019-2021 ----
library(readxl)

setwd("//phdata01/DROF_Data/DOH DATA/Housing/SHA/Original_data/2019 data extract/")
myfiles <- grep(".xls", list.files(), value = T)
mysheets <- excel_sheets(myfiles)

temp <- readxl::read_excel(myfiles, col_types = 'text', sheet = '2019')
write.csv(temp, "2019_sha_hcv_ph_complete_received_2023_01_03.csv", row.names = FALSE)
write.csv(temp, "//phdata01/DROF_Data/DOH DATA/Housing/SHA/Original_data/sha_hcv_ph_2019.csv", row.names = FALSE)
rm(temp)

temp <- readxl::read_excel(myfiles, col_types = 'text', sheet = '2020')
write.csv(temp, "//phdata01/DROF_Data/DOH DATA/Housing/SHA/Original_data/2020 data extract/2020_sha_hcv_ph_complete_received_2023_01_03.csv", row.names = FALSE)
write.csv(temp, "//phdata01/DROF_Data/DOH DATA/Housing/SHA/Original_data/sha_hcv_ph_2020.csv", row.names = FALSE)
rm(temp)

temp <- readxl::read_excel(myfiles, col_types = 'text', sheet = '2021')
write.csv(temp, "//phdata01/DROF_Data/DOH DATA/Housing/SHA/Original_data/2021 data extract/2021_sha_hcv_ph_complete_received_2023_01_03.csv", row.names = FALSE)
write.csv(temp, "//phdata01/DROF_Data/DOH DATA/Housing/SHA/Original_data/sha_hcv_ph_2021.csv", row.names = FALSE)
rm(temp)


