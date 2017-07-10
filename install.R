##installation code for tidyJunyi package
library(devtools)

##old version, which supports bigquery legacy SQL + dplyr 0.5 (not recommended)
install_url("https://github.com/peishenwu/tidyJunyi/raw/master/tidyJunyi_0.76.tgz")

##new version, which supports bigquery legacy SQL + dplyr 0.6 and above
install_url("https://github.com/peishenwu/tidyJunyi/raw/master/tidyJunyi_1.2.tgz")
